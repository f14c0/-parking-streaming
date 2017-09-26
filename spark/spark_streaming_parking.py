#!/usr/bin/python

# spark-submit --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar
# spark_streaming_parking.py localhost:2181 parking-test
import json
import pprint
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pykafka import KafkaClient
from kafka import KafkaProducer
import json
import sys
import pprint


def parking_status_in_kafka(status_counts):
    client = KafkaClient(hosts='34.209.151.179:9092')
    topic = client.topics['parking-test-summary']

    for status_count in status_counts:
        with topic.get_producer() as producer:
            producer.produce(json.dumps(status_count))


#zkQuorum, topic = sys.argv[1:]
zkQuorum = '34.209.151.179:2181'
topic = 'parking-test'
sc = SparkContext(appName="KafkaOrderCount")
# Batch duration is  set for 2 seconds
ssc = StreamingContext(sc, 30)
# Consumer group is "spark-streaming-consumer", per topic partitions to consume is 1
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
lines = kvs.map(lambda x: x[1])

# Show how many menssages in this batch
lines.count().map(lambda x:'Messages in this batch: %s' % x).pprint()

# sum by zones
zones_count_dstream = lines.map(lambda line:(line.split(",")[1],int(line.split(",")[3])))
zones_count_dstream.pprint()

parking_zone_count = zones_count_dstream.reduceByKey(lambda a, b: a+b)

parking_zone_count.pprint()

status_count = lines.map(lambda line: line.split(",")[1]) \
              .map(lambda zone_id: (zone_id, 1)) \
              .reduceByKey(lambda a, b: a+b)
status_count.pprint()
#status_count.foreachRDD(lambda rdd: rdd.foreachPartition(parking_status_in_kafka))
parking_zone_count.foreachRDD(lambda rdd: rdd.foreachPartition(parking_status_in_kafka))


ssc.start()
ssc.awaitTermination()