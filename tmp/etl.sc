//package com.awsproserv.kafkaandsparkstreaming
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object ClickstreamSparkstreaming {

  def main(args: Array[String]) {

    // if (args.length < 2) {
    //   System.err.println(s"""Ò
    //     |Usage: ClickstreamSparkstreaming <brokers> <topics>
    //     |  <brokers> is a list of one or more Kafka brokers
    //     |  <brokers> is a list of one or more Kafka topics to consume from
    //     |
    //     """.stripMargin)
    //   System.exit(1)
    // }

    //val Array(brokers, topics) = args


    val sparkConf = new SparkConf().setAppName("DirectKafkaClickstreams")
    // Create context with 10-second batch intervals
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct Kafka stream with brokers and topics
    //val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "b-1.task3cluster.vnnhgt.c6.kafka.us-east-1.amazonaws.com:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka-streaming-example",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topic2transactions")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    val lines = messages.map(_._2)

    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    // Drop the table if it already exists
    spark.sql("DROP TABLE IF EXISTS csmessages_hive_table")
    // Create the table to store your streams
    spark.sql("CREATE TABLE csmessages_hive_table ( recordtime string, eventid string, url string, ip string ) STORED AS TEXTFILE")
// Convert RDDs of the lines DStream to DataFrame and run a SQL query
    lines.foreachRDD { (rdd: RDD[String], time: Time) =>

    import spark.implicits._
      // Convert RDD[String] to RDD[case class] to DataFrame

     val messagesDataFrame = rdd.map(_.split(",")).map(w => Record(w(0), w(1), w(2), w(3))).toDF()

      // Creates a temporary view using the DataFrame
      messagesDataFrame.createOrReplaceTempView("csmessages")

      //Insert continuous streams into Hive table
      spark.sql("INSERT INTO TABLE csmessages_hive_table SELECT * FROM csmessages")

      // Select the parsed messages from the table using SQL and print it (since it runs on drive display few records)
      val messagesqueryDataFrame =
      spark.sql("SELECT * FROM csmessages")
      println(s"========= $time =========")
      messagesqueryDataFrame.show()
    }
  // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}
/** Case class for converting RDD to DataFrame */
case class Record(recordtime: String,eventid: String,url: String,ip: String)
