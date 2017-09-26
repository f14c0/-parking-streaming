#!/usr/bin/env python
import os, logging, time, csv
from kafka import  KafkaProducer


class Producer():
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers='54.190.9.240:9092')
        dir = os.path.dirname(__file__)
        filename = os.path.join(dir, 'sample-4.txt')
        with open('sample-4.txt','rb') as f:
            reader = csv.reader(f)
            skip_line=False
            for row in reader:
                if not skip_line:
                    skip_line=True
                    continue
                producer.send('parking-test', ','.join(row))
                print ','.join(row)
                time.sleep(1)


def main():
    producer = Producer()
    producer.run()

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )

main()

