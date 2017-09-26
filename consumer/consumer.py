
#!/usr/bin/env python
import threading, logging, time

from kafka import KafkaConsumer
from kafka.errors import KafkaError


class Consumer():
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='34.209.151.179:9092',
                                 auto_offset_reset='latest')
        consumer.subscribe(['parking-test-summary'])
        while True:
            msg = consumer.poll(50)
            time.sleep(2)
            print msg

def main():
    consumer = Consumer()
    consumer.run()


if __name__ == "__main__":
    #logging.basicConfig(
    #    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    #    level=logging.INFO
    #    )
    main()


