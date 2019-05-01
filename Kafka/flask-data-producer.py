import atexit
import logging
import json
import time
import random
import datetime
import requests

from googlefinance import getQuotes
from apscheduler.schedulers.background import BackgroundScheduler

from flask import (
    Flask,
    request,
    jsonify
)
# from flask.ext.cors import CORS, cross_origin

from kafka import KafkaProducer
from kafka.errors import (
    KafkaError,
    KafkaTimeoutError
)

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.INFO)

app = Flask(__name__)
# cors = CORS(app, resources={r"/": {"origins": "*"}})
# app.config['CORS_HEADERS'] = 'Content-Type'
# CORS(app)
app.config.from_envvar('ENV_CONFIG_FILE')
kafka_broker = app.config['CONFIG_KAFKA_ENDPOINT']
topic_name = app.config['CONFIG_KAFKA_TOPIC']

producer = KafkaProducer(
    bootstrap_servers=kafka_broker
)

schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()

symbols = set()


def shutdown_hook():
    """
    a shutdown hook to be called before the shutdown
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
    try:
        logger.info('shutdown scheduler')
        schedule.shutdown()
    except Exception as e:
        logger.warn('Failed to shutdown scheduler, caused by: %s', e.message)


# def fetch_price(symbol):
#     """
#     helper function to retrieve stock data and send it to kafka
#     :param symbol: symbol of the stock
#     :return: None
#     """
#     logger.debug('Start to fetch stock price for %s', symbol)
#     try:
#         price = json.dumps(getQuotes('AAPL'))
#         logger.debug('Retrieved stock info %s', price)
#         producer.send(topic=topic_name, value=price, timestamp_ms=time.time())
#         logger.info('Sent stock price for %s to Kafka', symbol)
#     except KafkaTimeoutError as timeout_error:
#         logger.warn('Failed to send stock price for %s to kafka, caused by: %s', (symbol, timeout_error.message))
#     except Exception:
#         logger.warn('Failed to fetch stock price for %s', symbol)

def fetch_price( symbol):
    """
    helper function to retrieve stock data and send it to kafka
    :param producer: instance of a kafka producer
    :param symbol: symbol of the stock
    :return: None
    """
    logger.debug('Start to fetch stock price for %s', symbol)
    try:
        # price = json.dumps(getQuotes(symbol))
        # symbol = "GOOG"
        url = 'https://investors-exchange-iex-trading.p.rapidapi.com/stock/' + symbol + '/book'
        response = requests.get(url,
                                headers={
                                    "X-RapidAPI-Host": "investors-exchange-iex-trading.p.rapidapi.com",
                                    "X-RapidAPI-Key": "cd4641fc4emshec67ca2f66d3209p108dd9jsn9b428d096e8b"
                                }
                                )
        price = response.json()

        price = price['quote']['latestPrice']* 0.01 * random.randint(50,150)
        logger.info("price:"+ str(price))
        # price = random.randint(30, 120)
        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%MZ')
        payload = ('[{"StockSymbol":"%s","LastTradePrice":%d,"LastTradeDateTime":"%s"}]' % (symbol, price, timestamp)).encode('utf-8')

        logger.debug('Retrieved stock info %s', price)
        producer.send(topic=topic_name, value=payload, timestamp_ms=time.time())
        logger.debug('Sent stock price for %s to Kafka', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn('Failed to send stock price for %s to kafka, caused by: %s', (symbol, timeout_error.message))
    except Exception:
        logger.warn('Failed to fetch stock price for %s', symbol)


@app.route('/<symbol>', methods=['POST'])
def add_stock(symbol):
    if not symbol:
        return jsonify({
            'error': 'Stock symbol cannot be empty'
        }), 400
    if symbol in symbols:
        pass
    else:
        # symbol = symbol.encode('utf-8')
        symbols.add(symbol)


        schedule.add_job(fetch_price, 'interval', [symbol], seconds=3, id=symbol)
        logger.info('Add stock retrieve job %s, %s' % (symbol, type(symbol)))
    return jsonify(results=list(symbols)), 200


@app.route('/<symbol>', methods=['DELETE'])
# @cross_origin(origin='*',headers=['Content-Type','Authorization'])
def del_stock(symbol):
    if not symbol:
        return jsonify({
            'error': 'Stock symbol cannot be empty'
        }), 400
    if symbol not in symbols:
        pass
    else:
        symbols.remove(symbol)
        schedule.remove_job(symbol)
    return jsonify(results=list(symbols)), 200

if __name__ == '__main__':
    atexit.register(shutdown_hook)
    app.run(host='0.0.0.0', port=app.config['CONFIG_APPLICATION_PORT'])
