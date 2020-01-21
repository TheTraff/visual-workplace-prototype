import pika
import time
import csv
import json
import sys

# rabbit MQ connection and exchange declaration
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='events',
                         exchange_type='fanout')


if __name__ == '__main__':

    event_list = []
    with open(sys.argv[1]) as event_file:
        reader = csv.reader(event_file, delimiter=',')
        for row in reader:
            event_dict = {}
            event_dict['id'] = row[0]
            event_dict['event_type'] = row[1]
            event_dict['f_id'] = row[3]
            event_dict['event_time'] = row[5]
            event_dict['created_by'] = row[12]
            channel.basic_publish(exchange='events',
                                  routing_key='',
                                  body=json.dumps(event_dict))
            print(f" [x] Sent {row[1]} for fulfillment {row[3]}")


