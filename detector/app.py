"""Example Kafka consumer."""

import os
import json
import os

from kafka import KafkaConsumer
from kafka import KafkaConsumer, KafkaProducer

TRANSACTIONS_TOPIC = os.environ.get('TRANSACTIONS_TOPIC')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TRANSACTIONS_TOPIC = os.environ.get('TRANSACTIONS_TOPIC')
LEGIT_TOPIC = os.environ.get('LEGIT_TOPIC')
FRAUD_TOPIC = os.environ.get('FRAUD_TOPIC')

def is_suspicious(transaction: dict) -> bool:
    # TODO add better rules and/or AI superpowers
    """Determine whether a transaction is suspicious."""
    return transaction['amount'] >= 900


@@ -20,7 +22,12 @@ def is_suspicious(transaction: dict) -> bool:
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda value: json.loads(value),
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    for message in consumer:
        transaction: dict = message.value
        if is_suspicious(transaction):
            print({'type': 'potential_fraud', 'transaction': transaction})
        topic = FRAUD_TOPIC if is_suspicious(transaction) else LEGIT_TOPIC
        producer.send(topic, value=transaction)
        print(topic, transaction)  # DEBUG
