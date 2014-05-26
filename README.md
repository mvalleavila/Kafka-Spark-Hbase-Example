# SparkOnALog
## Overview
This project is intended to show examples of how to integrate Flume -> Spark Streaming -> HBase


##Functionality
There is a Main class in com.cloudera.sa.sparkonalog.  This class has the following functions:

* *SimpleFlumeAvroClient* : This is a very basic Flume Avro Client 
* *RandomWordEventFlumeAvroClient* : This is a very simple Flume Avro Client that sends two chars seperated by a spaced.  It is intended to be used to feed the word count examples.
* *HBaseCreateTable*: Creates to simple table for streaming counters to be persisted
* *SparkStreamingFromFlumeExample* : This is a simple Flume Client to Spark Streaming implementation that counts the number of flume events in the last window.
* *SparkStreamingFromFlumeToHBaseExample* : This does a word count on the flume events getting sent to it.  It will flush the word counts to HBase every N seconds.

##Build
mvn clean package

##Usage
### Flume Client -> Spark Streaming Test
java -cp SparkOnALog.jar com.cloudera.sa.sparkonalog.Main SparkStreamingFromFlumeExample local[2] 127.0.0.1 4141

java -cp SparkOnALog.jar com.cloudera.sa.sparkonalog.Main SimpleFlumeAvroClient 127.0.0.1 4141 100

### Flume Client -> Spark Streaming -> Flush to HBase Test
java -cp SparkOnALog.jar com.cloudera.sa.sparkonalog.Main HBaseCreateTable testTable c

java -cp SparkOnALog.jar com.cloudera.sa.sparkonalog.Main SparkStreamingFromFlumeToHBaseExample local[2] 127.0.0.1 4141 testTable c

java -cp SparkOnALog.jar com.cloudera.sa.sparkonalog.Main RandomWordEventFlumeAvroClient 127.0.0.1 4141 200000

