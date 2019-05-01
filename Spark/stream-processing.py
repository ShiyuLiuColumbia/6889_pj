# - read from any kafka
# - do computation
# - write back to kafka

import atexit
import logging
import json
import sys
import time
from joblib import load
import numpy as np

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from sklearn.preprocessing import normalize

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)

topic = None
target_topic = None
window_topic = None
brokers = None
kafka_producer = None


def shutdown_hook(producer):
    """
    a shutdown hook to be called before the shutdown
    :param producer: instance of a kafka producer
    :return: None
    """
    try:
        logger.info('Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by: %s', e.message)
def window_stream(stream):
    def window_send_to_kafka(rdd):
        results = rdd.collect()

        for r in results:
            data = json.dumps(
                {
                    'symbol': r[0],
                    'timestamp': time.time(),
                    'variance':r[1],
                    'average': r[2],
                    'trend':r[3],
                    'prediction':str(r[4])
                }
            )
            try:
                logger.info('Sending window average price %s to kafka' % data)
                kafka_producer.send(window_topic, value=bytes(data, 'utf-8'))
            except KafkaError as error:
                logger.warn('Failed to send window stock price to kafka, caused by: %s', error.message)

    def window_pair(data):
        # bytes(nonlat, 'utf-8')
        # record = json.loads(data[1])[0]
        record = json.loads(bytes(data[1], 'utf-8'))[0]
        price = float(record.get('LastTradePrice'))
        return record.get('StockSymbol'), (price, price ** 2,[price], 1)


    stream.map(window_pair).window(30, 10).reduceByKey(
        lambda a, b: (a[0] + b[0],  # summation
                      a[1] + b[1],  # square summation
                      a[2] + b[2],  # list
                      a[3] + b[3]   # count
                      )).map(lambda p: (
                                       p[0], # symbol
                                       p[1][1] / p[1][3] - (p[1][0] / p[1][3]) ** 2,  # variance
                                       p[1][0] / p[1][3],  # average
                                        np.polyfit(p[1][2], np.r_[0:len(p[1][2])], 1)[0] #trend
                                       )).map(lambda q: (
                                                        q[0],   # symbol
                                                        q[1], # variance
                                                        q[2], # average
                                                        q[3], #trend
                                                        clf.predict(normalize([[q[1],q[2], q[3]]], axis=1))[0]) #prediction
                                              ).foreachRDD(window_send_to_kafka)
    # stream.map(pair).reduceByKeyAndWindow(lambda x, y: x[0] + y[0], 30, 10).foreachRDD(send_to_kafka)
        # (lambda a, b: (a[0] + b[0], # summation
        #               a[1] + b[1], # square summation
        #               a[2] + b[2]), 30, 10)
        # .map(lambda p: (p[0], # summation
        #                                            p[1][1]/p[1][2] - (p[1][0]/p[1][2])**2, #variance
        #                                            p[1][0]/p[1][2] #average
        #                                            )).foreachRDD(send_to_kafka)
def process_stream(stream):

    def send_to_kafka(rdd):
        results = rdd.collect()
        for r in results:
            data = json.dumps(
                {
                    'symbol': r[0],
                    'timestamp': time.time(),
                    'average': r[2],
                    'variance':r[1]
                }
            )
            try:
                logger.info('Sending average price %s to kafka' % data)
                kafka_producer.send(target_topic, value=bytes(data, 'utf-8'))
            except KafkaError as error:
                logger.warn('Failed to send average stock price to kafka, caused by: %s', error.message)


    def pair(data):
# bytes(nonlat, 'utf-8')
        # record = json.loads(data[1])[0]
        record = json.loads(bytes(data[1], 'utf-8'))[0]
        price = float(record.get('LastTradePrice'))
        return record.get('StockSymbol'), (price, price**2, 1)

    # stream.map(pair).reduceByKey(lambda a, b: a[0] + b[0], a[1] + b[1]).map(lambda k, v: (k, v[0]/v[1])).foreachRDD(send_to_kafka)
    stream.map(pair).reduceByKey(
        lambda a, b: (a[0] + b[0], # summation
                      a[1] + b[1], # square summation
                      a[2] + b[2])).map(lambda p: (p[0], # summation
                                                   p[1][1]/p[1][2] - (p[1][0]/p[1][2])**2, #variance
                                                   p[1][0]/p[1][2] #average
                                                   )).foreachRDD(send_to_kafka)
if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: stream-process.py [topic] [target-topic] [window-topic] [broker-list]")
        exit(1)

    # - create SparkContext and StreamingContext
    sc = SparkContext("local[2]", "StockAveragePrice")
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 5)

    topic, target_topic, window_topic, brokers = sys.argv[1:]
    clf = load("../Sklearn/model.joblib")

    # - instantiate a kafka stream for processing
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': brokers})
    process_stream(directKafkaStream)
    window_stream(directKafkaStream)

    # - instantiate a simple kafka producer
    kafka_producer = KafkaProducer(
        bootstrap_servers=brokers
    )

    # - setup proper shutdown hook
    atexit.register(shutdown_hook, kafka_producer)

    ssc.start()
    ssc.awaitTermination()
