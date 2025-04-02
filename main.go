package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
)

// ChannelPool manages a pool of RabbitMQ channels
type ChannelPool struct {
	channels chan *amqp.Channel
	conn     *amqp.Connection
}

// NewChannelPool creates and initializes a new pool of RabbitMQ channels
// Parameters:
//   - conn: The RabbitMQ connection
//   - size: Number of channels to create in the pool
//
// Returns: A new ChannelPool instance
func NewChannelPool(conn *amqp.Connection, size int) *ChannelPool {
	pool := &ChannelPool{
		channels: make(chan *amqp.Channel, size),
		conn:     conn,
	}
	for i := 0; i < size; i++ {
		ch, err := conn.Channel()
		if err != nil {
			log.Fatal(err)
		}
		pool.channels <- ch
	}
	return pool
}

// Get retrieves a channel from the pool
func (p *ChannelPool) Get() *amqp.Channel {
	return <-p.channels
}

// Put returns a channel to the pool
func (p *ChannelPool) Put(ch *amqp.Channel) {
	p.channels <- ch
}

var (
	db           *sql.DB
	rabbitConn   *amqp.Connection
	channelPool  *ChannelPool
	publishMutex = &sync.Mutex{}

	// Monitoring metrics
	requestCount    int64
	totalLatency    int64
	requestCountMux sync.RWMutex

	// Configuration
	port = "8080"
)

// Data structures for the application
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

// Main function: Initializes the application, sets up connections, and starts the HTTP server
func main() {
	// Database Connection Setup
	// Establishes connection to MySQL database with connection pooling configuration
	dsn := "root:your_password@tcp(database-2.cqzfidh4zvkc.us-west-2.rds.amazonaws.com:3306)/your_database_name"
	if val := os.Getenv("DB_DSN"); val != "" {
		dsn = val
	}
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(50)
	db.SetConnMaxLifetime(time.Hour)

	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}

	// Database Schema Setup
	// Creates necessary tables if they don't exist
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

	// Create index for review lookup optimization
	err = ensureReviewIndexExists(db)
	if err != nil {
		log.Fatalf("Failed to ensure index: %v", err)
	}

	// RabbitMQ Setup
	// Initializes RabbitMQ connection and channel pool for message queue processing
	rabbitURL := "amqp://guest:guest@localhost:5672/"
	if val := os.Getenv("RABBITMQ_URL"); val != "" {
		rabbitURL = val
	}
	rabbitConn, err = amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	// Create channel pool instead of single channel
	channelPool = NewChannelPool(rabbitConn, 50)

	// Initialize queue on all channels
	for i := 0; i < 50; i++ {
		ch := channelPool.Get()
		_, err = ch.QueueDeclare("reviews", true, false, false, false, nil)
		if err != nil {
			log.Fatalf("Failed to declare queue: %v", err)
		}
		channelPool.Put(ch)
	}

	// Consumer Setup
	// Starts multiple consumer goroutines to process review messages
	consumerCount := 50
	if val := os.Getenv("CONSUMER_COUNT"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			consumerCount = n
		}
	}
	// Start a new goroutine for each consumer to process messages concurrently
	for i := 0; i < consumerCount; i++ {
		go startConsumer(i)
	}

	// HTTP Server Setup with Gin Framework
	// Configures routes and middleware for the REST API
	r := gin.Default()

	// Health Check Endpoint
	// Provides a simple endpoint to check if the service is running
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// Album Management Endpoints
	// Handles album creation and retrieval
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

	// Review Management Endpoints
	// Handles review submission and retrieval
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
		saveReview(review)
		c.Status(200)
		return

		// Get a channel from the pool
		ch := channelPool.Get()
		defer channelPool.Put(ch)

		err = ch.Publish("", "reviews", false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})

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

	// Server Start
	// Initializes the HTTP server on the specified port
	r.Run(":" + port)
}

// Database Operations
// saveReview persists a review to the database with a timeout context
func saveReview(review Review) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, "INSERT INTO reviews (album_id, action) VALUES (?, ?)", review.AlbumID, review.Action)
	if err != nil {
		log.Printf("Failed to insert review: %v", err)
	}
}

// Message Queue Consumer
// startConsumer initializes a consumer that processes review messages from RabbitMQ
func startConsumer(id int) {
	// Get a channel from the pool
	ch := channelPool.Get()
	defer channelPool.Put(ch) // Return the channel to the pool when done

	// Declare queue and set QoS
	ch.QueueDeclare("reviews", true, false, false, false, nil)
	ch.Qos(1, 0, false)

	// Start consuming messages
	msgs, err := ch.Consume("reviews", "", false, false, false, false, nil)
	if err != nil {
		log.Printf("Consumer %d: failed to start consuming: %v", id, err)
		return
	}

	// Process messages
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

// Database Index Management
// ensureReviewIndexExists creates an index on the reviews table if it doesn't exist
func ensureReviewIndexExists(db *sql.DB) error {
	rows, err := db.Query(`SHOW INDEX FROM reviews WHERE Key_name = 'idx_album_action'`)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		log.Println("Index idx_album_action already exists")
		return nil
	}

	_, err = db.Exec(`CREATE INDEX idx_album_action ON reviews(album_id, action)`)
	if err != nil {
		return err
	}
	log.Println("Created index idx_album_action")
	return nil
}
