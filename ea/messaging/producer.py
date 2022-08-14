from __future__ import annotations

import json
from abc import ABC, abstractmethod

from kafka import KafkaProducer


class MessageProducer(ABC):
    @abstractmethod
    def send(self, message) -> None:
        pass


class KafkaProducerWrapper(MessageProducer):
    def __init__(self, configuration):
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=configuration['bootstrap_servers'],
            value_serializer=lambda m: json.dumps(m).encode('ascii')
        )
        self.topic = configuration['topic']

    def send(self, message):
        self.kafka_producer.send(self.topic, message)
        self.kafka_producer.flush()
