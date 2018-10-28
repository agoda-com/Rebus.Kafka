docker stop kafka-rebus
docker rm kafka-rebus
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=127.0.0.1 --env ADVERTISED_PORT=9092 --name kafka-rebus spotify/kafka