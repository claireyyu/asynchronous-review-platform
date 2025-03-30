#!/bin/bash

echo "Clearing RabbitMQ queue..."

sudo docker exec -i rabbitmq rabbitmqctl purge_queue reviews

echo "✅ RabbitMQ 'reviews' queue cleared!"

echo "Clearing MySQL tables..."

sudo docker exec -i mysql-db mysql -uroot -pasync123 reviews <<EOF
TRUNCATE TABLE reviews;
TRUNCATE TABLE albums;
EOF

echo "✅ Tables cleared!"
