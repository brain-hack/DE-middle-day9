#!/usr/bin/env python
# coding: utf-8

# In[5]:


from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
import logging

logging.basicConfig()

a = AdminClient({'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094'})


def create_topics(a, topics):
    """ Create topics """

    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=2) for topic in topics]
    # Call create_topics to asynchronously create topics, a dict
    # of <topic,future> is returned.
    fs = a.create_topics(new_topics)

    # Wait for operation to finish.
    # Timeouts are preferably controlled by passing request_timeout=15.0
    # to the create_topics() call.
    # All futures will finish at the same time.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))

topic_names = ["streaming-in","processing_output"]

create_topics(a,topics=topic_names)

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()


df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094").option("subscribe", "trade").load()


words = df.select(explode(split(df.value, ' ')).alias('word'))


# Generate running word count
wordCounts = words.groupBy('word').count()

query = df.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094").option("topic", "processing_output").option("checkpointLocation", "/tmp/sparkstream/checkpoint").start()



query.awaitTermination()

