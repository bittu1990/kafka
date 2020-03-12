#Databricks notebook source

#Installing and starting Kafka and Zookeeper
wget https://archive.apache.org/dist/kafka/0.10.2.2/kafka_2.10-0.10.2.2.tgz

tar -xzf kafka_2.10-0.10.2.2.tgz
ls -lrt

cd kafka_2.10-0.10.2.2
cd bin
./zookeeper-server-start.sh config/zookeeper.properties

./kafka-server-start.sh config/server.properties

#installing kafka-python
pip install kafka-python


