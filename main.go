package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
)

var (
	db           *sql.DB
	rabbitConn   *amqp.Connection
	publishChan  *amqp.Channel
	publishMutex = &sync.Mutex{}
)

type Album struct {
	ID       int    `json:"id"`
	Artist   string `json:"artist"`
	Title    string `json:"title"`
	Year     int    `json:"year"`
	ImageURL string `json:"image_url"`
}

type Review struct {
	AlbumID int    `json:"album_id"`
	Action  string `json:"action"` // "like" or "dislike"
}

func main() {
	// MySQL connection
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		log.Fatal("DB_DSN environment variable not set")
	}
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}

	// Create albums and reviews tables
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS albums (
		id INT AUTO_INCREMENT PRIMARY KEY,
		artist VARCHAR(255),
		title VARCHAR(255),
		year INT,
		image_url TEXT
	);`)
	if err != nil {
		log.Fatalf("Failed to create albums table: %v", err)
	}
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS reviews (
		id INT AUTO_INCREMENT PRIMARY KEY,
		album_id INT,
		action ENUM('like', 'dislike'),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`)
	if err != nil {
		log.Fatalf("Failed to create reviews table: %v", err)
	}

	// RabbitMQ setup
	rabbitURL := os.Getenv("RABBIT_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}
	rabbitConn, err = amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	publishChan, err = rabbitConn.Channel()
	if err != nil {
		log.Fatalf("Failed to create publishing channel: %v", err)
	}
	_, err = publishChan.QueueDeclare("reviews", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// Start consumers
	consumerCount := 2
	if val := os.Getenv("CONSUMER_COUNT"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			consumerCount = n
		}
	}
	for i := 0; i < consumerCount; i++ {
		go startConsumer(i)
	}

	// Gin routes
	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	r.POST("/albums", func(c *gin.Context) {
		var album Album
		if err := c.ShouldBindJSON(&album); err != nil {
			c.JSON(400, gin.H{"error": "Invalid album data"})
			return
		}
		res, err := db.Exec("INSERT INTO albums (artist, title, year, image_url) VALUES (?, ?, ?, ?)",
			album.Artist, album.Title, album.Year, album.ImageURL)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		id, _ := res.LastInsertId()
		c.JSON(200, gin.H{"id": id})
	})

	r.POST("/review/:action/:albumID", func(c *gin.Context) {
		action := c.Param("action")
		albumIDStr := c.Param("albumID")

		if action != "like" && action != "dislike" {
			c.JSON(400, gin.H{"error": "Action must be 'like' or 'dislike'"})
			return
		}

		albumID, err := strconv.Atoi(albumIDStr)
		if err != nil {
			c.JSON(400, gin.H{"error": "Invalid albumID"})
			return
		}

		review := Review{
			AlbumID: albumID,
			Action:  action,
		}

		body, _ := json.Marshal(review)

		async := os.Getenv("ASYNC_MODE")
		if async == "false" {
			saveReview(review)
			c.Status(200)
			return
		}

		publishMutex.Lock()
		err = publishChan.Publish("", "reviews", false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
		publishMutex.Unlock()

		if err != nil {
			log.Printf("Failed to publish message: %v", err)
			c.JSON(500, gin.H{"error": "Failed to publish message"})
			return
		}

		c.Status(202)
	})

	r.GET("/review/:albumID", func(c *gin.Context) {
		albumIDStr := c.Param("albumID")
		albumID, err := strconv.Atoi(albumIDStr)
		if err != nil {
			c.JSON(400, gin.H{"error": "Invalid albumID"})
			return
		}

		var likes, dislikes int

		err = db.QueryRow("SELECT COUNT(*) FROM reviews WHERE album_id = ? AND action = 'like'", albumID).Scan(&likes)
		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to count likes"})
			return
		}

		err = db.QueryRow("SELECT COUNT(*) FROM reviews WHERE album_id = ? AND action = 'dislike'", albumID).Scan(&dislikes)
		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to count dislikes"})
			return
		}

		c.JSON(200, gin.H{
			"album_id": albumID,
			"likes":    likes,
			"dislikes": dislikes,
		})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	r.Run(":" + port)
}

func saveReview(review Review) {
	_, err := db.Exec("INSERT INTO reviews (album_id, action) VALUES (?, ?)", review.AlbumID, review.Action)
	if err != nil {
		log.Printf("Failed to insert review: %v", err)
	}
}

func startConsumer(id int) {
	ch, err := rabbitConn.Channel()
	if err != nil {
		log.Fatalf("Consumer %d: failed to open channel: %v", id, err)
	}
	// defer ch.Close()

	ch.QueueDeclare("reviews", true, false, false, false, nil)
	ch.Qos(1, 0, false)

	msgs, err := ch.Consume("reviews", "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Consumer %d: failed to start consuming: %v", id, err)
	}

	for msg := range msgs {
		var review Review
		if err := json.Unmarshal(msg.Body, &review); err == nil {
			saveReview(review)
			msg.Ack(false)
			log.Printf("Consumer %d processed review for album %d [%s]", id, review.AlbumID, review.Action)
		} else {
			log.Printf("Consumer %d failed to parse message: %s", id, msg.Body)
			msg.Nack(false, false)
		}
	}
}
