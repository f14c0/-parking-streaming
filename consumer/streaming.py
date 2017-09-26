from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from datetime import datetime
import json

# Create a StreamingContext with batch interval of 3 second
#sc = SparkContext("spark://localhost:7077", "parking_app")
sc = SparkContext(appName="parking_app")
sc.setLogLevel("WARN")
# Batch duration is  set for 3 seconds
ssc = StreamingContext(sc, 3)
# Consumer group is spark-streaming, per topic partitions to consume is 1
topic = "parking-topic-test"
kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "parking-test",
                                      {topic: 1})


# this will fail if the message isnt a valid json
parsed = kafkaStream.map(lambda v:json.loads(v[1]))
parsed.count().map(lambda x:'Messages in this batch: %s' % x).pprint()
authors_dstream = parsed.map(lambda tweet: tweet['user']['screen_name'])

author_counts = authors_dstream.countByValue()
author_counts.pprint()
author_counts_sorted_dstream = author_counts.transform(
    (lambda foo:foo.sortBy(
        lambda x:(-x[1]))
     ))
author_counts_sorted_dstream.pprint()
top_five_authors = author_counts_sorted_dstream.transform\
    (lambda rdd:sc.parallelize(rdd.take(5)))
top_five_authors.pprint()
ssc.start()
ssc.awaitTermination()





#raw = kafkaStream.flatMap(lambda kafkaS: [kafkaS])
#time_now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
#clean = raw.map(lambda xs: xs[1].split(","))

# Match cassandra table fields with dictionary keys
# this reads input of format: x[partition, timestamp]
#my_row = clean.map(lambda x: {
#      "testid": "test",
#     "time1": x[1],
#      "time2": time_now,
#      "delta": (datetime.strptime(x[1], '%Y-%m-%d %H:%M:%S.%f') -
#       datetime.strptime(time_now, '%Y-%m-%d %H:%M:%S.%f')).microseconds,
#      })

# save to cassandra
#my_row.saveToCassandra("KEYSPACE", "TABLE_NAME")

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate