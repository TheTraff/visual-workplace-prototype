import time
import csv
import json
import sys
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import psycopg2
import psycopg2.extras

print(" [x] Setting up database connection")
db_password = os.environ['PGPASSWORD']
remote_conn = psycopg2.connect(f"postgresql://reports:{db_password}@archimedes.bi.proctoru.com/proctoru_production")
local_conn = psycopg2.connect("postgresql://master@localhost/datamart")

"""
        now() as time,
        proctor_level as csr_level,
        avg(current_wait)/60 as avg_wait,
        max(current_wait)/60 as max_wait,
        sum(case when current_wait <= 300 then 1 else 0 end) as zero_to_five,
        sum(case when current_wait > 300 and current_wait <= 600 then 1 else 0
        end) as five_to_ten,

        sum(case when current_wait > 600 and current_wait <= 900 then 1 else 0
        end) as ten_to_fifteen,

        sum(case when current_wait > 900 and current_wait <= 1200 then 1 else 0
        end) as fifteen_to_twenty,

        sum(case when current_wait > 1200 and current_wait <= 1500 then 1 else 0
        end) as twenty_to_fifteen,

        count(*) as wait_count
"""


def time_buckets():

    query = '''
	select
        now() as time,
        sum(case when state = 'WAITING' then 1 else 0 end) as wating_count,
        sum(case when state = 'PRECHECK' then 1 else 0 end) as precheck_count,


    from
    (select
    fulfillments.id as f_id,
    fulfillments.uuid as f_uuid,
    iterations.id as iteration_id,
    institutions.id as institution_id,
    iterations.type as iteration_type,
    proctor_levels.name as proctor_level,
    (
    select 
      tags.name 
    from taggings 
    left join tags on tags.id = taggings.tag_id
    where taggings.taggable_id = institutions.id
    and context = 'service_types' and taggable_type = 'Institution') as
    service_type,

    welcome_event.time as welcome,
    (extract(epoch from lmi_connected_event.time) - extract(epoch from
    welcome_event.time)) as precheck_time,

    lmi_connected_event.time as lmi_connected,

    (extract(epoch from launch_start_event.time) - extract(epoch from
    lmi_connected_event.time)) as wait_time,

    (extract(epoch from now()::timestamp) - extract(epoch from
    lmi_connected_event.time)) as current_wait,

    launch_start_event.time as launch_start,

    (extract(epoch from launch_end_event.time) - extract(epoch from
    launch_start_event.time)) as launch_time,

    launch_end_event.time as launch_end,

    disconnect_event.time as disconnect,

    case
      when (
        lmi_connected_event.time is not null and
        launch_start_event.time is null and 
        launch_end_event.time is null and 
        welcome_event.time is not null and 
        disconnect_event.time is null) then 'WAITING'
      when (
        welcome_event.time is not null and 
        lmi_connected_event.time is null and launch_start_event.time is null and
      launch_end_event.time is null) then 'PRECHECK'
      when (launch_start_event.time is not null and launch_end_event.time
      is null) then 'LAUNCHING' 
    end as state,

    array_agg(tags.name) as proctor_skills

  from
    fulfillments

  left join lateral (
      select min(created_at) as time
      from events
      where events.fulfillment_id = fulfillments.id
      and events.type = 'Event::Welcome') welcome_event on true
  left join lateral (
      select min(created_at) as time
      from events
      where events.fulfillment_id = fulfillments.id
      and events.type = 'Event::LmiConnected') lmi_connected_event on true
  left join lateral (
      select min(created_at) as time
      from events
      where events.fulfillment_id = fulfillments.id
      and events.type = 'Event::LaunchStart') launch_start_event on true
  left join lateral (
      select min(created_at) as time
      from events
      where events.fulfillment_id = fulfillments.id
      and events.type = 'Event::LaunchEnd') launch_end_event on true
  left join lateral (
      select min(created_at) as time
      from events
      where events.fulfillment_id = fulfillments.id
      and events.type = 'Event::HardDisconnection') disconnect_event on true
  left join lateral (
      select min(created_at) as time
      from events
      where events.fulfillment_id = fulfillments.id
      and events.type = 'Event::GetHelp') get_help_event on true

  left join reservations on fulfillments.reservation_id = reservations.id
  left join iterations on reservations.iteration_id = iterations.id
  left join exams on iterations.exam_id = exams.id
  left join institutions on exams.institution_id = institutions.id

left join proctor_levels on proctor_levels.id = institutions.proctor_level_id
  LEFT JOIN taggings on taggings.taggable_id = institutions.id AND
  context = 'skills' AND taggable_type = 'Institution' 
  LEFT JOIN tags on tags.id = taggings.tag_id


  where fulfillments.actual_started_at is not null
  and fulfillments.actual_ended_at is null
  and fulfillments.actual_started_at > (current_timestamp - interval '3 hours')
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
) in_progress
;

    '''

    cur = remote_conn.cursor()

    cur.execute(query)
    remote_conn.commit()
    results = cur.fetchall()

    #sum = 0
    #for record in results:
    #    if record[11] is not None:
    #        sum += record[11]
    #print(sum)
    cur.close()
    return results

def update_datamart_table(fulfillments):
    cur = local_conn.cursor()
    records_list_template = ','.join(['%s'] * len(fulfillments))
    query = '''
        insert into in_progress_fulfillments (fulfillment_id, fulfillment_uuid, iteration_id,
        institution_id, iteration_type, proctor_level, service_type, now,
        welcome, precheck_time, lmi_connected, wait_time,
        current_wait,launch_start, launch_time, launch_end, disconnected,
        state, proctor_skills) values {} on conflict
        (fulfillment_id) do update set welcome = excluded.welcome,
        lmi_connected = excluded.lmi_connected,
        now = excluded.now,
        launch_start = excluded.launch_start,
        launch_end = excluded.launch_end,
        precheck_time = excluded.precheck_time,
        wait_time = excluded.wait_time,
        current_wait = excluded.current_wait,
        launch_time = excluded.launch_time,
        state = excluded.state;
    '''.format(records_list_template)

    cur.execute(query, fulfillments)


    local_conn.commit()


    cur.close()

def format_record(record):

    record_str = ""
    for col in record:
        record_str += f"| {col} "

    return record_str





if __name__ == '__main__':

    print("Starting REFRESH")
    while(True):
        time.sleep(5)
        print("retrieving records")
        records = time_buckets()
        for record in records:
            print(format_record(record))
        #print("dumping records")
        #update_datamart_table(fulfillments)











