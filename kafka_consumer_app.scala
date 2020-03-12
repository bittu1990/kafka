// Databricks notebook source
//importing all required libraries
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka10._
import org.apache.spark.streaming.kafka10.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka10.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


//Setting up kafka params
val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "group1",
  "auto.offset.reset" -> "latest",
  "enable.auto.commit" -> (false: java.lang.Boolean))
  
val topic = Array("DurstexpreesTopic")
 

//Setting up Spark DS
val batchInterval = Seconds(5)
val ssc = new StreamingContext(sc, batchInterval)
  
val stream = KafkaUtils.createDirectStream[String, String](
  ssc,
  PreferConsistent,
  Subscribe[String, String](topic, kafkaParams)
)

val message = stream.map(record => (record.key, record.value))
  

//Message Schema
val userSchema = StructType(
  StructField("id", IntegerType, false) ::
  StructField("first_name", StringType, false) ::
  StructField("last_name", StringType, false) :: 
  StructField("email", StringType, false) ::
  StructField("gender", StringType, false) ::
  StructField("ip_address", StringType, false) ::
  StructField("date", StringType, false) ::
  StructField("country", StringType, false) ::
)
  


// COMMAND ----------

messages.foreachRDD { rdd =>

  val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
  import spark.implicits._
  
  val rawDF = rdd.toDF("userData")  
  val dfUserData = rawDF.select(from_json($"userData", userSchema) as "data").select("data.*")

  dfUserData.cache()
  dfUserData.createOrReplaceTempView("userData")
  val uniqueID = spark.sql("select count(distinct (id)) from userData")
  
  println("1. Number of unique users: " + uniqueID.toString )
  
  val countryCount = spark.sql("select country, count(id) from userData where country is not null group by country order by count(id),country")
  
  println("Least represented country: ")
  val leastCountry = countryCount.show(1)
  
  println("Most represented country: ")
  val mostCounrty = countryCount.sort(desc("count")).show(1)
  
  val genderDistribution = spark.sql("select country, count(id) from userData where country is not null group by country order by count(id),country")

  println("Top 5 most represented countries: ")
  val topFiveCountry = spark.sql("select country, gender, count(1) as user_count from userData group by country, gender order by count(1) desc ").show(10)

}

ssc.start()
ssc.awaitTermination()

