package com.njkol.music.analytics.algos

import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra.CassandraSQLContext
import com.njkol.music.analytics.constants.Constants._
import org.apache.spark.sql.functions.{explode,col}

class MusicAnalyticsDataFrameAlgos(sc: SparkContext) {

  private val csc = new CassandraSQLContext(sc)
  // Set the Cassandra keyspace to be used
  csc.setKeyspace(KEYSPACE)

  import csc.implicits._

  def populatePerformersByStyle() {

    val performers = csc.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "spark_demo", "table" -> "performers"))
      .load

    val performerAndStyles = performers.select("name", "styles")
      .withColumn("style", explode($"styles"))
      .withColumn("performer", $"name")
      .drop("styles").drop("name")
      .select("style", "performer")

    performerAndStyles.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "spark_demo", "table" -> "performers_by_style"))
      .save
  }

  def populateAlbumsByDecadeAndCountry() {

    // Read from albums table
    val albums = csc.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "spark_demo", "table" -> "albums"))
      .load.select("performer", "year")
      .where($"year" >= 1900)

    val performers = csc.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "spark_demo", "table" -> "performers"))
      .load.select("name", "country")
      .where($"country".isNotNull && $"country" != "Unknown")
      
     val res1 = albums.join(performers,col("performer") === col("name"))
     .drop("performer")
     .select("name", "country","year")

    // Join albums with performers table
  }
}