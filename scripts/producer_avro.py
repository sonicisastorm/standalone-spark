from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import time

schema_path = "schemas/user.avsc"
key_schema = avro.load(schema_path)
value_schema = avro.load(schema_path)

producer = AvroProducer(
    {
        'bootstrap.servers': 'kafka:9092',
        'schema.registry.url': 'http://schema-registry:8081'
    },
    default_key_schema=key_schema,
    default_value_schema=value_schema
)

events = [
    {"id": 1, "name": "Alice", "city": "Baku", "zipcode": "AZ1000"},
    {"id": 2, "name": "Bob", "city": "Ganja", "zipcode": "AZ2000"},
]

for e in events:
    producer.produce(topic='users', key={"id": e["id"]}, value=e)
    print("Produced:", e)
    time.sleep(1)

producer.flush()
