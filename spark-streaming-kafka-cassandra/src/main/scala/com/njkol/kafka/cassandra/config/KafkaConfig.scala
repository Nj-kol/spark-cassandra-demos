package com.njkol.kafka.cassandra.config

import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.collection.mutable.HashMap

object KafkaConfig {

  val consumerGrp = "wordCountConsumer"
  val kafkBrokers = "localhost:9092"
  
  val kafkaConsumerConfig = Map[String, String](
    "metadata.broker.list" -> kafkBrokers,
    "group.id" -> consumerGrp,
    "enable.auto.commit" -> "false",
    "refresh.leader.backoff.ms" -> "1000")
}



