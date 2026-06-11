docker exec -it kafka-practice kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders_created --property parse.key=true --property key.separator=:
