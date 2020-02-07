import pika
import time
import csv
import json
import sys
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

real_time_scale = 1
time_interval = 1

# rabbit MQ connection and exchange declaration
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='events',
                         exchange_type='fanout')

print(" [x] Setting up database connection")
db_password = os.environ['PGPASSWORD']
engine = create_engine(f"postgresql://reports:{db_password}@archimedes.bi.proctoru.com/proctoru_production")
conn = engine.connect()


def get_events_for_interval(start_time, end_time):

    sql = '''
        select * from events 
        where created_at between :start_time and :end_time
        and events.type in ('Event::Welcome', 'Event::LmiConnected', 'Event::LaunchStart', 'Event::LaunchEnd', 'Event::FulfillmentEnded') order by created_at asc;
    '''

    result = conn.execute(text(sql), start_time = start_time, end_time = end_time)
    return result.fetchall()


def publish_event(event):
    """
    pushes an event object to rabbitmq
    """
    event_dict = {}

    event_dict['id'] = event['id']
    event_dict['event_type'] = event['type']
    event_dict['f_id'] = event['fulfillment_id']
    event_dict['event_time'] = datetime.strftime(event['created_at'], '%Y-%m-%d %H:%M:%S')
    event_dict['created_by'] = event['created_by_id']

    channel.basic_publish(exchange='events',
                          routing_key='',
                          body=json.dumps(event_dict))
    print(f"\t[x] Sent {event['type']} for fulfillment {event['fulfillment_id']}")
    

if __name__ == '__main__':


    start_time = datetime(2019, 12, 12, 0, 0 ,0)
    end_time = datetime(2019, 12, 12, 11, 59, 59)

    real_time_interval = timedelta(seconds = real_time_scale)

    current_time = start_time + real_time_interval
    while current_time < end_time:
        events = get_events_for_interval(current_time - real_time_interval, current_time)
        print(current_time)
        for event in events:
            publish_event(event)

        # increment the time tracker and sleep for the simulation time step
        current_time = current_time + real_time_interval
        time.sleep(time_interval)

    events = get_events_for_interval(start_time, end_time)

    print(len(events))


    #event_list = []
    #with open(sys.argv[1]) as event_file:
    #    reader = csv.reader(event_file, delimiter=',')
    #    for row in reader:
    #        event_dict = {}
    #        event_dict['id'] = row[0]
    #        event_dict['event_type'] = row[1]
    #        event_dict['f_id'] = row[3]
    #        event_dict['event_time'] = row[5]
    #        event_dict['created_by'] = row[12]
    #        channel.basic_publish(exchange='events',
    #                              routing_key='',
    #                              body=json.dumps(event_dict))
    #        print(f" [x] Sent {row[1]} for fulfillment {row[3]}")
    #        time.sleep(2)


