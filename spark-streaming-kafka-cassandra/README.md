
# A simple application showcasing Kafka+ Spark + Cassandra

## Create Cassandra

use demo;

CREATE TABLE sensordata (
id text,
day text,
hour text,
reading text,
primary key((id, day), hour)
) WITH CLUSTERING ORDER BY (hour DESC) 
AND COMPACTION = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};
		
## Create Kafka topic

kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic sensordata

## Post messages to Kafka

kafka-console-producer.bat --broker-list localhost:9092 --topic sensordata

1,2018-10-25,1,12
1,2018-10-25,2,23
1,2018-10-25,3,31
1,2018-10-25,4,27
2,2018-10-25,1,11
2,2018-10-25,2,13
2,2018-10-25,3,18
2,2018-10-25,4,16

## See the data

select * from sensordata where id = '1' and day = '2018-10-25;