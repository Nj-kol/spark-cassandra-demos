package com.njkol.music.analytics.constants

object Constants {

  val CASSANDRA_HOST_NAME_PARAM = "spark.cassandra.connection.host"
  val CASSANDRA_IP = "localhost"
  val LOCAL_MODE = "local"
  
  val KEYSPACE = "spark_demo"

  val PERFORMERS = "performers"
  val ALBUMS = "albums"
  val ALBUMS_BY_COUNTRY = "albums_by_country"
  val PERFORMERS_BY_STYLE = "performers_by_style"
  val PERFORMERS_DISTRIBUTION_BY_STYLE = "performers_distribution_by_style"
  val TOP_10_STYLES = "top10_styles"
  val ALBUMS_BY_DECADE_AND_COUNTRY = "albums_by_decade_and_country"
  val ALBUMS_BY_DECADE_AND_COUNTRY_SQL = "albums_by_decade_and_country_sql"
}