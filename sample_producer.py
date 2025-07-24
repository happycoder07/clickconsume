from confluent_kafka import Producer
import json

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'ipfix_logs'

p = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

with open('sample.json') as f:
    data = f.read()

p.produce(KAFKA_TOPIC, value=data)
p.flush()
print("Sample log sent to Kafka.") 