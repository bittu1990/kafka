##Installation and Setup (install folder):

-	I have used Databricks community cloud to build this solution  - https://community.cloud.databricks.com
-	This cluster comes with 1 node, python3, spark2.4.5, scala2.11
-	I have installed Kafka2.2 from a mirror site as 2.4 is having issues with Scala_11 log4j while starting kafka service.
-	Installation steps are given in the `Install_Kafka` file.
-	Required Maven library for integration of Spark an Kafka is available on the cloud infra(screenshot attached).


##Python Solution (python folder):

-	I have 3 python code files  - producer, consumer and the main_data_process application.
-	Kafka topic - `DurstexpreesTopic`
-	Producer app is reading the JSON file and transmitting the entire list in a single message to DurstexpreesTopic
-	Consumer app is subscring to the DurstexpreesTopic and reading the message and supplying it to the main_data_process.
-	I have Pandas to convert the received messages into DataFrame to do the calculations further.
- 	Results screenshot is attached.


##Spark Solution (spark folder):

-	I have used the same producer as used for the Python application
-	Created a new consumer app with Spark DS which receives the messages from the Kafka topic - DurstexpreesTopic
-	Transform the Spark DS into Spark DataFrame
- 	Used SparkQL for the calculations futher.
-	Result screenshot is attached.

Note: I was facing integration issues with Kafka and Spark mainly with the maven build. I have assumed that the consumer will read the messages from the kafka topic and loaded the JSON directly into spark DS to move ahead.
