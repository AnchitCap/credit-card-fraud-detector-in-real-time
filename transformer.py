from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def handle_rdd(rdd):
    if not rdd.isEmpty():
        global ss
        #df = ss.createDataFrame(rdd, schema=['transaction_id', 'user_id', 'card_number','amount','description','transaction_type','vendor','approval_status'])
        spark = SparkSession(ss)
        df = spark.read.json(rdd)
        df.show()
        df.write.saveAsTable(name='default.TransactionsDynamoDB', format='hive', mode='append')

sc = SparkContext(appName="TransactionsStreaming")
ssc = StreamingContext(sc, 5)

ss = SparkSession.builder \
        .appName("TransactionsStreaming") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()

ss.sparkContext.setLogLevel('WARN')

ks = KafkaUtils.createDirectStream(ssc, ['topic2transactions'], {'metadata.broker.list': 'b-1.task3cluster.vnnhgt.c6.kafka.us-east-1.amazonaws.com:9092,b-2.task3cluster.vnnhgt.c6.kafka.us-east-1.amazonaws.com:9092'})

lines = ks.map(lambda x: x[1])

#transform = lines.map(lambda trxn: (trxn.split(',')[0], trxn.split(',')[1], trxn.split(',')[2], trxn.split(',')[3], trxn.split(',')[4], trxn.split(',')[5],trxn.split(',')[6],trxn.split(',')[7]))
transform = lines.map(lambda trxn :trxn)
transform.foreachRDD(handle_rdd)

ssc.start()
ssc.awaitTermination()
