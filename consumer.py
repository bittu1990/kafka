# Databricks notebook source
from kafka import KafkaConsumer
import sys

def consumer_app():
    """
    This will create a Kafka Consumer to access the data published over the required topic
    :param self: No parameters
    :return: List of JSON
    """
    bootstrap_servers = ['localhost:9092']
    topicName = 'DurstexpreesTopic'
    consumer = KafkaConsumer(topicName, group_id='group1', bootstrap_servers=bootstrap_servers,
                             auto_offset_reset='earliest', consumer_timeout_ms=2000)
    json_dict = {}
    for message in consumer:
        consumer.commit()
        json_dict = message.value.decode('ascii')
    consumer.close()
    return json_dict