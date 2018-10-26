package com.njkol.music.analytics

import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import com.njkol.music.analytics.connectors.CassandraConnecter

import com.njkol.music.analytics.algos.MusicAnalyticsAlgos
import com.njkol.music.analytics.algos.MusicAnalyticsDataFrameAlgos


/**
 * @author Nilanjan Sarkar
 */
object MusicAnalyticsLauncher extends App with CassandraConnecter {

  val sc = buildSparkContext("MusicAnalytics")

   val algos = new MusicAnalyticsAlgos(sc)
  //algos.populatePerformersByStyle()
  //algos.populatePerformersDistributionByStyle()
  // algos.populateTop10Styles
  algos.populateAlbumsByDecadeAndCountry

  //val algos = new MusicAnalyticsDataFrameAlgos(sc)
 // algos.populatePerformersByStyle
  
  sc.stop()
}