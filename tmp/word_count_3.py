sc = SparkContext(appName=”PythonStreamingDirectKafkaWordCount”)
#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json

# Every 5 seconds
sc = SparkContext(appName='PythonStreamingDirectKafkaWordCount')
ssc = StreamingContext(sc, 5)

lines = KafkaUtils.createStream(ssc, 'z-2.task3cluster.vnnhgt.c6.kafka.us-east-1.amazonaws.com:2181,z-3.task3cluster.vnnhgt.c6.kafka.us-east-1.amazonaws.com:2181,z-1.task3cluster.vnnhgt.c6.kafka.us-east-$

# Split each line in each batch into words
words = lines.flatMap(lambda line: line[1].split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the elements of each RDD generated in this DStream to the console
wordCounts.pprint()

# Start the computation
ssc.start()

# Wait for the computation to terminate
ssc.awaitTermination()
