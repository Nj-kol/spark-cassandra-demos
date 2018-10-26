package com.njkol.kafka.cassandra

import java.util.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import _root_.kafka.serializer.StringDecoder
import com.njkol.kafka.cassandra.config.KafkaConfig

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._

object KafkaCassandraDemo extends App {

  case class SensorData(id: String, day: String, hour: String, reading: String)

  val sparkConf = new SparkConf().setAppName("SensorDataStream")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "localhost")

  val ssc = new StreamingContext(sparkConf, Seconds(5))

  val topics: Set[String] = "sensordata".split(",").map(_.trim).toSet
  val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, KafkaConfig.kafkaConsumerConfig, topics)

  val sensorDataStream = directKafkaStream.map(_._2.split(","))
    .map(in => SensorData(in(0), in(1), in(2), in(3)))

  /** Saves the raw data to Cassandra - raw table. */
  sensorDataStream.saveToCassandra("demo", "sensordata")
  
  ssc.start()
  ssc.awaitTermination()
}