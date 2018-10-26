package com.njkol.music.analytics.connectors

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{ SparkConf, SparkContext }
import com.njkol.music.analytics.constants.Constants._

trait CassandraConnecter {

  def buildSparkContext(appName: String): SparkContext = {

    val conf = new SparkConf(true)
      .setAppName(appName)
      .setMaster(LOCAL_MODE)
      .set(CASSANDRA_HOST_NAME_PARAM, CASSANDRA_IP)
    new SparkContext(conf)
  }
}