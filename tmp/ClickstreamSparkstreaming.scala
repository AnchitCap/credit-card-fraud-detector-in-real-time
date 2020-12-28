package com.awsproserv.kafkaandsparkstreaming

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

    if (args.length < 2) {
      System.err.println(s"""
        |Usage: ClickstreamSparkstreaming <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf().setAppName("DirectKafkaClickstreams")
    // Create context with 10 second batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)

    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    // Drop the tables if it already exists
    spark.sql("DROP TABLE IF EXISTS transactions_hive_table")

    // Create the tables to store your streams
    val query1 = "CREATE EXTERNAL TABLE transactions_hive_table (transaction_id STRING,user_id STRING, card_number STRING, amount STRING)"
    var query2 = "LOCATION 's3://task-3-spark-scripts-bucket/trxtablefolder'"
    var query = query1 + query2;
    // val query2 = "STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'"
    // val query3 = "TBLPROPERTIES("
    // val query4 = "\"dynamodb.table.name\" = \"task3-transactions\","
    // val query5 = "\"dynamodb.column.mapping\"=\"transaction_id:transaction_id,user_id:user_id,card_number:card_number,amount:amount\""
    // val query6 = ")"
    // val query = query1+query2+query3+query4+query5+query6

    spark.sql(query)

    // Convert RDDs of the lines DStream to DataFrame and run SQL query
    lines.foreachRDD { (rdd: RDD[String], time: Time) =>

      import spark.implicits._
      // Convert RDD[String] to RDD[case class] to DataFrame

      val messagesDataFrame = rdd.map(_.split(",")).map(w => Record(w(0), w(1), w(2), w(3))).toDF()

      // Creates a temporary view using the DataFrame
      messagesDataFrame.createOrReplaceTempView("trxmessages")

      //Insert continuous streams into hive table
      spark.sql("INSERT OVERWRITE TABLE transactions_hive_table SELECT * FROM trxmessages")

      // select the parsed messages from table using SQL and print it (since it runs on drive display few records)
      val messagesqueryDataFrame =
      spark.sql("select * from trxmessages")
      println(s"========= $time =========")
      messagesqueryDataFrame.show()
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}
/** Case class for converting RDD to DataFrame */
case class Record(transaction_id: String,user_id: String,card_number: String,amount: String)
