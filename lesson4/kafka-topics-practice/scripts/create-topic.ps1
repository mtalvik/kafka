docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders_created --partitions 3 --replication-factor 1
