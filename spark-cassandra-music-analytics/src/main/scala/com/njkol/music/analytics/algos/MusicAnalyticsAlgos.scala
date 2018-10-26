package com.njkol.music.analytics.algos

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext

import com.njkol.music.analytics.constants.Constants._

import java.io.Serializable

class MusicAnalyticsAlgos(sc: SparkContext) extends Serializable {

  def populatePerformersByStyle() {

    val rdd: CassandraRDD[CassandraRow] = sc.cassandraTable(KEYSPACE, PERFORMERS)
    val performerAndStyles: RDD[(String, String)] =
      rdd.map[(String, List[String])](row => (row.getString("name"), row.getList[String]("styles").toList))
        .flatMap[(String, String)] { case (performer, styles) => styles.map(style => (style, performer)) }

    //Save data back to Cassandra
    performerAndStyles.saveToCassandra(KEYSPACE, PERFORMERS_BY_STYLE, SomeColumns("style", "performer"))
  }

  def populatePerformersDistributionByStyle() {

    val PERFORMERS_TYPES = Map("Person" -> "artist", "Group" -> "group")
    /*
     * Read columns "type" and "styles" from table 'performers'
     * and save them as (String,List[String]) RDDs
     * using the .as((_:String,_:List[String])) type conversion function
     * Normalize the performer type by filtering out 'Unknown' types
     */
    val rdd: RDD[(String, List[String])] = sc.cassandraTable(KEYSPACE, PERFORMERS)
      .select("type", "styles")
      .as((_: String, _: List[String]))
      .map { case (performer_type, style) => (PERFORMERS_TYPES.getOrElse(performer_type, "Unknown"), style) }
      .filter { case (performer_type, _) => performer_type != "Unknown" }

    /*
    * Transform the previous tuple RDDs into a key/value RDD (PairRDD) of type
    * ((String,String),Integer). The (String,String) pair is the key(performer type,style)
    * The Integer value should be set to 1 for each element of the RDD
    */
    val pairs: RDD[((String, String), Int)] = rdd.flatMap {
      case (performer_type, styles) => styles.map(style => ((performer_type, style), 1))
    }

    /*
     * Reduce the previous tuple of ((performer type,style),1) by
     * adding up all the 1's into a  ((performer type,style),count)
     */
    val reduced: RDD[((String, String), Int)] = pairs.reduceByKey { case (left, right) => left + right }

    /*
     * Flatten the ((performer type,style),count) into
     *  (performer type,style,count)
     */
    val aggregated: RDD[(String, String, Int)] = reduced.map {
      case ((performer_type, style), count) => (performer_type, style, count)
    }

    //Save data back to the performers_distribution_by_style table
    aggregated.saveToCassandra(KEYSPACE, PERFORMERS_DISTRIBUTION_BY_STYLE, SomeColumns("type", "style", "count"))
  }

  def populateTop10Styles() {
    val cc = new CassandraSQLContext(sc)

    /*
     * Read data from 'performers_distribution_by_style' table
     */
    val groupRDD: RDD[(String, String, Int)] = cc.cassandraSql(s"""
      SELECT *
      FROM $KEYSPACE.$PERFORMERS_DISTRIBUTION_BY_STYLE
      WHERE style != 'Unknown' AND type = 'group'
      """)
      .map(row => (row.getString(0), row.getString(1), row.getInt(2)))
      .sortBy(tuple => tuple._3, false, 1)

    groupRDD.cache()

    val soloArtistRDD: RDD[(String, String, Int)] = cc.cassandraSql(s"""
      SELECT *
      FROM $KEYSPACE.$PERFORMERS_DISTRIBUTION_BY_STYLE
      WHERE style != 'Unknown' AND type = 'artist'
      """)
      .map(row => (row.getString(0), row.getString(1), row.getInt(2)))
      .sortBy(tuple => tuple._3, false, 1)

    soloArtistRDD.cache()

    // Count total number of groups  having styles that are not in the top 10
    val otherStylesCountForGroup: Int = groupRDD
      .collect() //Fetch the whole RDD back to driver program
      .drop(10) //Drop the first 10 top styles
      .map { case (_, _, count) => count } //Extract the count
      .sum //Sum up the count

    // Count total number of artists having styles that are not in the top 10
    val otherStylesCountForArtist: Int = soloArtistRDD
      .collect() //Fetch the whole RDD back to driver program
      .drop(10) //Drop the first 10 top styles
      .map { case (_, _, count) => count } //Extract the count
      .sum //Sum up the count

    // Take the top 10 styles for groups, with a count for all other styles
    val top10Groups = groupRDD.take(10) :+ ("group", "Others", otherStylesCountForGroup)

    // Take the top 10 styles for artists, with a count for all other styles
    val top10Artists = soloArtistRDD.take(10) :+ ("artist", "Others", otherStylesCountForArtist)

    /*
     * Remark: by calling take(n), all the data are shipped back to the driver program
     * the output of take(n) is no longer an RDD but a simple Scala collection
     */

    // Merge both list and save back to Cassandra
    sc.parallelize(top10Artists.toList ::: top10Groups.toList)
      .saveToCassandra(KEYSPACE, TOP_10_STYLES, SomeColumns("type", "style", "count"))
  }

  /**
   *
   */
  def populateAlbumsByDecadeAndCountry() {

    // Read from albums table
    val albums: RDD[(String, Int)] = sc.cassandraTable(KEYSPACE, ALBUMS)
      .select("performer", "year")
      .as((_: String, _: Int))
      .filter { case (_, year) => year >= 1900 }

    // Join albums with performers table
    val join: RDD[(String, (String, Int))] = albums.repartitionByCassandraReplica(KEYSPACE, PERFORMERS, 1)
      .joinWithCassandraTable[(String, String)](KEYSPACE, PERFORMERS, SomeColumns("name", "country"))
      .on(SomeColumns("name"))
      .filter { case (_, (_, country)) => country != null && country != "Unknown" }
      .map { case ((performer, year), (name, country)) => (name, (country, year)) }

    val finale = join
      //RDD into a ((decade,country),1) 
      .map { case (performer, (country, year)) => ((s"${(year / 10) * 10}-${((year / 10) + 1) * 10}", country), 1) }
      //Reduce by key to count the number of occurrence for each key (decade,country)
      .reduceByKey { case (left, right) => left + right }
      //Flatten the tuple to (decade,country,count) triplet
      .map { case ((decade, country), count) => (decade, country, count) }

    finale.saveToCassandra(KEYSPACE, ALBUMS_BY_DECADE_AND_COUNTRY, SomeColumns("decade", "country", "album_count"))
  }

}
 