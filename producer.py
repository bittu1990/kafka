# Databricks notebook source
from kafka import KafkaProducer
import json

bootstrap_servers = ['localhost:9092']
topicName = 'DurstexpreesTopic'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         linger_ms=10)

with open("mock_data.json") as file:
    json_msg = json.loads(file.read())
    
producer.send(topicName, json_msg)
producer.flush()