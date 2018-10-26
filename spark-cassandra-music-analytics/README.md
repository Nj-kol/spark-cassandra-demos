
# Music Analytics

## Data preparation

* Go to the folder main/data

* Execute $CASSANDRA_HOME/bin/cqlsh -f music.cql from this folder
  
  It should create the keyspace spark_demo and some tables
  the script will then load into Cassandra the content of
  performers.csv and albums.csv
   
## Scenarios 

**Example 1** 

* In this example, we read data from the `performers` table to extract performers
  and styles into the `performers_by_style` table

**Example 2 **

* In this example, we read data from the `performers` table, group styles by performer
  for aggregation. The results are saved back into the `performers_distribution_by_style` table

**Example 3**

* Similar to Example2 we only want to extract the top 10 styles for artists 
  and groups and save the results into the `top10_styles` table
  
**Example 4**

* In this example, we want to know, for each decade, the number of albums
  released by each artist, group by their origin country
  
* For this we join the table `performers` with `albums`.

* The results are saved back into the `albums_by_decade_and_country` table


Reference
=========
https://github.com/doanduyhai/Cassandra-Spark-Demo