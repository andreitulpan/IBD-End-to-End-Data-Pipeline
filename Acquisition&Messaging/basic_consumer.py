from kafka import KafkaConsumer
import json

BROKER_URL = '#################'
USERNAME = '#################'
PASSWORD = '#################'
CA_FILE = '#################'

consumer = KafkaConsumer(
    'sensor_data',
    bootstrap_servers=[BROKER_URL],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-256',
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    ssl_cafile=CA_FILE,
    ssl_check_hostname=True
)

print("Listening securely...")
for message in consumer:
    print(f"Received: {message.value}")