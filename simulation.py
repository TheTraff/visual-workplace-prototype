import enum
import pika
import time
import json
import sys
import time
import pandas as pd
from datetime import datetime
from sqlalchemy import Column, DateTime, String, Integer, Enum, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

print(" [x] Setting up database connection")
engine = create_engine('postgresql://master@localhost/datamart')

# set up the session
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()


class States(enum.Enum):
    PRECHECKS = 0
    WAITING = 1
    LAUNCHING = 2
    TESTING = 3
    DONE = 4
    UNKNOWN = 5

state_changes = {
    States.PRECHECKS: {
        'Event::Welcome': States.PRECHECKS,
        'Event::LmiConnected': States.WAITING,
        'Event::LaunchStart': States.PRECHECKS,
        'Event::LaunchEnd': States.PRECHECKS,
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
    },
    States.UNKNOWN: {
        'Event::Welcome': States.PRECHECKS,
        'Event::LmiConnected': States.WAITING,
        'Event::LaunchStart': States.LAUNCHING,
        'Event::LaunchEnd': States.TESTING,
        'Event::FulfillmentEnded': States.DONE
    }
}

Base = declarative_base()

# database classes
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
        return state_changes[self.state][event.type]

    def get_state(current_state, event):
        if current_state is None:
            return state_changes[States.UNKNOWN][event.type]
        return state_changes[current_state][event.type]

    def update_state(self, trigger_event, new_state):
        trigger_event_time = datetime.strptime(trigger_event.time, '%Y-%m-%d %H:%M:%S.%f')
        time_in_state = (trigger_event_time - self.state_start_time).seconds

        state_change = StateChange(f_id=self.f_id,
                                   old_state=self.state,
                                   new_state=state_changes[self.state][trigger_event.type],
                                   time_in_state=time_in_state,
                                   change_time=trigger_event.time)
        return state_change


class StateChange(Base):
    __tablename__ = 'state_changes'
    id = Column(Integer, primary_key=True)
    f_id = Column(Integer)
    old_state = Column(Enum(States))
    new_state = Column(Enum(States))
    time_in_state = Column(Integer)
    change_time = Column(DateTime)

class Event(Base):
    __tablename__ = 'events'
    id = Column(Integer, primary_key=True)
    type = Column(String)
    f_id = Column(Integer)
    time = Column(DateTime)
    created_by = Column(String)

# drop and recreate the tables so I don't have to do it
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

def get_state(event):
    return state_changes[self.state][event.type]


def handle_event(event):
    #print(f" [x] Processing {event.type} for {event.fulfillment_id}")

    f_id = event.fulfillment_id
    event_obj = Event(id=event.id, type=event.type, f_id=event.fulfillment_id, time=event.created_at, created_by=event.created_by_id)

    session.add(event_obj)

    fulfillment = session.query(Fulfillment).filter_by(f_id=f_id).first()
    
    if not fulfillment:
        # We found a new fulfillment to track
        fulfillment = Fulfillment(f_id=f_id, state=Fulfillment.get_state(None, event_obj), state_start_time=event.created_at)
        #print(f"\t[x] Found new fulfillment {fulfillment.f_id} in state {fulfillment.state}")
        session.add(fulfillment)
        session.commit()
    else:
        new_state = Fulfillment.get_state(fulfillment.state, event_obj)
        if fulfillment.state != new_state: # state change
            state_change = fulfillment.update_state(event_obj, new_state)
            session.add(state_change)

        #print(f"\t[*] Fulfillment {fulfillment.f_id} moving from state {fulfillment.state} to {new_state}")
        fulfillment.state = new_state

    session.commit()
    #time.sleep(1)



if __name__ == "__main__":

    events_df = pd.read_csv(sys.argv[1]).sort_values(by='created_at')

    time_counts = []

    current_count_sql = '''
        select state, max(state_start_time), count(*) from current_fulfillments group by 1;
    '''

    print("[x] processing events, please stand by")
    for event in events_df.iterrows():
        if datetime.strptime(event[1].created_at, '%Y-%m-%d %H:%M:%S.%f') > datetime.strptime('2020-02-07 00:21:17.242125', '%Y-%m-%d %H:%M:%S.%f'):
            print("\t[*] found the most wait times")
            break
        handle_event(event[1])

        result = session.execute(text(current_count_sql)).fetchall()
        time_counts.append(result)


    # find the time with the most 

    wait_counts = {}

    #print("[x] finding wait counts")
    #for time_segment in time_counts:
    #    for count in time_segment:
    #        if count[0] == 'WAITING':
    #            wait_counts[count[1]] = count[2]

    #max_count = max(wait_counts.values())
    #for key, val in wait_counts.items():
    #    if val == max_count:
    #        print(f"max count is {val} at time {key}")








