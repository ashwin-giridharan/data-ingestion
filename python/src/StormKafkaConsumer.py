from __future__ import print_function

import sys
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: ashwin_kafka_consumer.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    HDFS_URL = "hdfs://ashwin-centos65-1.vpc.cloudera.com:8020"
    HDFS_BASE_DIR = "/user/ingestdata"

    sc = SparkContext(appName="DataIngestionPython")
    ssc = StreamingContext(sc, 10)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    
    print("******Received data from kafka producer. total lines: ",lines.count())
    
    #lines.pprint(10)
    def writeToHDFS(rdd):
        if rdd.isEmpty() == False:
            print("******Received data from kafka producer. Top lines: ",rdd.take(1))
            rdd.saveAsTextFile(HDFS_URL+HDFS_BASE_DIR+"/pyfile"+str(int(time.time())))
        else:
            print("******No data received in this time frame")
        return None

    lines.foreachRDD(writeToHDFS)

    ssc.start()
    ssc.awaitTermination()