import enum
import pika
import time
import json
from datetime import datetime
from sqlalchemy import Column, DateTime, String, Integer, Enum, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class States(enum.Enum):
    ENTERING = 0
    WAITING = 1
    LAUNCHING = 2
    TESTING = 3
    DONE = 4
    UNKNOWN = 5

state_changes = {
    States.ENTERING: {
        'Event::Welcome': States.ENTERING,
        'Event::LmiConnected': States.WAITING,
        'Event::LaunchStart': States.ENTERING,
        'Event::LaunchEnd': States.ENTERING,
        'Event::FulfillmentEnded': States.DONE
    },
    States.WAITING: {
        'Event::Welcome': States.WAITING,
        'Event::LmiConnected': States.WAITING,
        'Event::LaunchStart': States.LAUNCHING,
        'Event::LaunchEnd': States.WAITING,
        'Event::FulfillmentEnded': States.DONE
    },
    States.LAUNCHING: {
        'Event::Welcome': States.LAUNCHING,
        'Event::LmiConnected': States.LAUNCHING,
        'Event::LaunchStart': States.LAUNCHING,
        'Event::LaunchEnd': States.TESTING,
        'Event::FulfillmentEnded': States.DONE
    },
    States.TESTING: {
        'Event::Welcome': States.TESTING,
        'Event::LmiConnected': States.TESTING,
        'Event::LaunchStart': States.TESTING,
        'Event::LaunchEnd': States.TESTING,
        'Event::FulfillmentEnded': States.DONE
    },
    States.DONE: {
        'Event::Welcome': States.DONE,
        'Event::LmiConnected': States.DONE,
        'Event::LaunchStart': States.DONE,
        'Event::LaunchEnd': States.DONE,
        'Event::FulfillmentEnded': States.DONE
    }
}


Base = declarative_base()

# rabbit MQ connection and exchange declaration
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='events',
                         exchange_type='fanout')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='events', queue=queue_name)


class Fulfillment(Base):
    __tablename__ = 'current_fulfillments'
    f_id = Column(Integer, primary_key=True)
    state = Column(Enum(States))
    state_start_time = Column(DateTime)

    def __repr__(self):
        return f"<Fulfillment(f_id={self.f_id}, state={self.state})>"

    def time_in_state(self, event):
        return (datetime.strptime(event.event_time, '%Y-%m-%d %H:%M:%S.%f')- self.state_start_time).seconds

    def change_state(self, event):
        return state_changes[self.state][event.event_type]

class FulfillmentHistory(Base):
    __tablename__ = 'fulfillment_history'
    id = Column(Integer, primary_key=True)
    f_id = Column(Integer)
    state = Column(Enum(States))
    time_in_state = Column(Integer)

class Event(Base):
    __tablename__ = 'events'
    id = Column(Integer, primary_key=True)
    event_type = Column(String)
    f_id = Column(Integer)
    event_time = Column(DateTime)
    created_by = Column(String)


print(" [x] Setting up database connection")
engine = create_engine('postgresql://master@localhost/datamart')
Base.metadata.create_all(engine)
Session = sessionmaker()
Session.configure(bind=engine)

def get_state(event):
    if event == 'Event::Welcome':
        return States.ENTERING
    if event == 'Event::LmiConnected':
        return States.WAITING 
    if event == 'Event::LaunchStart':
        return States.LAUNCHING
    if event == 'Event::LaunchEnd':
        return States.TESTING
    if event == 'Event::FulfillmentEnded':
        return States.DONE
    else:
        return States.UNKNOWN

def update_history(fulfillment, trigger_event):
    insert_stmt = '''
        insert into fulfillment_history (fulfillment_id, state, time_in_state)
        values (:fulfillment_id, :old_state, :time_in_state);
    '''
    history_entry = FulfillmentHistory(f_id=fulfillment.f_id, 
                                       state=fulfillment.state, 
                                       time_in_state=fulfillment.time_in_state(trigger_event))
    session = Session()
    session.add(history_entry)
    session.commit()
    session.close()

def handle_events_4(ch, method, properties, body):
    event = json.loads(body)
    event_type = event['event_type']
    f_id = event['f_id']
    event_time = event['event_time']
    print(f" [x] Processing {event['event_type']} for {event['f_id']}")

    print(event) 
    event = Event(**event)
    # get sqlalchemy session
    session = Session()

    session.add(event)

    fulfillment = session.query(Fulfillment).filter_by(f_id=f_id).first()
    
    if not fulfillment:
        # We found a new fulfillment to track
        fulfillment = Fulfillment(f_id=f_id, state=get_state(event_type), state_start_time=event_time)
        print(f"\t[x] Found new fulfillment {fulfillment.f_id} in state {fulfillment.state}")
        session.add(fulfillment)
        session.commit()
        session.close()
    else:
        new_state = fulfillment.change_state(event)
        print(f"\t[*] Fulfillment {fulfillment.f_id} moving from state {fulfillment.state} to {new_state}")
        fulfillment.state = new_state

    session.commit()
    session.close()


         
def handle_events_3(ch, method, properties, body):
    event = json.loads(body)
    event_type = event['event_type']
    f_id = event['f_id']
    event_time = event['event_time']
    print(f" [x] Processing {event['event_type']} for {event['f_id']}")

    print(event) 
    event = Event(**event)
    # get sqlalchemy session
    session = Session()

    session.add(event)

    new_state = get_state(event_type)
    fulfillment = session.query(Fulfillment).filter_by(f_id=f_id).first()
    
    if not fulfillment:
        fulfillment = Fulfillment(f_id=f_id, state=get_state(event_type), state_start_time=event_time)
        session.add(fulfillment)
        session.commit()
        session.close()
    else:
        old_state = fulfillment.state
        # if the new state is not unknown and the state has changed
        if new_state != States.UNKNOWN and old_state != new_state:
            update_history(fulfillment, event)
            fulfillment.state = new_state

    session.commit()
    session.close()

def handle_events_2(ch, method, properties, body):
    event = body.split(b' ')
    fulfillment_id = event[3].decode('ascii')
    event_type = event[1].decode('ascii')

    new_fulfillment = False

    if fulfillment_id not in [f.id for f in fulfillments.values()]:
        #new fulfillment 
        new_fulfillment = True
        fulfillments[fulfillment_id] = Fulfillment(fulfillment_id, event_type)
    else:
        # fulfillment we've already seen
        f_original_state = fulfillments[fulfillment_id].state
        fulfillments[fulfillment_id].trigger(event_type)

    f_new_state = fulfillments[fulfillment_id].state
    print(f_new_state)

    current_state = {}

    if not new_fulfillment:
        print(f" [x] fulfillment {fulfillment_id} moving from {f_original_state} to {f_new_state}")
    else:
        print(f" [x] New fulfillment {fulfillment_id} in state {f_new_state}")
    print("---------")
    for state in States:
        current_state[state] = len([f for f in fulfillments.values() if f.state == state])
        print(f"TTs {state.name}: {current_state[state]}")



        

def handle_events(ch, method, properties, body):
    event = body.split(b' ')
    fulfillment_id = event[3].decode('ascii')
    event_type = event[1].decode('ascii')

    new_fulfillment_flag = False

    print(f" [x] Processing {event_type} for {fulfillment_id}")

    if fulfillment_id not in fulfillments:
        fulfillments[fulfillment_id] = new_fulfillment()
        new_fulfillment_flag = True

    if event_type == 'Event::Welcome':
        fulfillments[fulfillment_id] = fulfillment_welcome()
    if event_type == 'Event::LMIConnected':
        fulfillments[fulfillment_id] = fulfillment_connected()
    if event_type == 'Event::LaunchStart':
        fulfillments[fulfillment_id] = fulfillment_launch_start()
    if event_type == 'Event::LaunchExamClicked':
        fulfillments[fulfillment_id] = fulfillment_launch_exam()
    if event_type == 'Event::LaunchEnd':
        del(fulfillments[fulfillment_id])
    else:
        print("unknown event: {event_type} for {fulfillment_id}")
   
    print("f_id    |  event       ", flush=True)
    for f_id, progress in fulfillments.items():
        if f_id == fulfillment_id:
            print(f"{f_id}| {progress} | ****")
        else:
            print(f"{f_id}| {progress} |")

    if not new_fulfillment_flag:
        time.sleep(5)

channel.basic_consume(queue=queue_name,
                      on_message_callback=handle_events_4,
                      auto_ack=True)

print(' [* Waiting for events. To exit press CTRL+C')
channel.start_consuming()

