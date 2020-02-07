# Visual Workplace Proof of Concept
A prototype project for the data processing portion of the Visual Workplace. This project is proof of concept, therefore any details related to how events are processed and how fulfillments are handled are NOT final and only for this project. Some simplifications have been made to create this prototype quickly. Any references to "datamart" are actually a locally running Postgres database

## Background
The visual workplace project is an upcoming project being taken on by the BI team. The final desired state of the project is a Tableau workbook (or possibly more than one workbook) that will be used as a dashboard to keep track of real-time metrics of the proctoring process. Initially, this will only include wait times but will be expanded in the future to include more metrics. To power this workbook, a data source is needed that will have real-time data about each ongoing fulfillment. That is what this proof of concept is for. To demonstrate an ability to have a live table of sorts that keeps track of where each test taker is along the proctoring process. 

## Tracking Schema
So far the prototype uses 3 tables:
* events: the table for storing raw events as they come in. (Not completely necessary, but was easy to implement)
* current_fulfillments: This table is for keeping track of the current state of fulfillment. Below are some example entries

| fulfillment_id | state                   | state_start_time    | 
|----------------|-------------------------|---------------------|
| 00000000       | ENTERING                | 2020-01-01 13:10:43 |
| 11111111       | TESTING                 | 2020-01-01 12:09:23 |
| 22222222       | LAUNCHING               | 2020-01-01 13:05:54 |
| 33333333       | WAITING                 | 2020-01-01 13:01:35 |
| 44444444       | WAITING                 | 2020-01-01 12:59:11 |

* fulfillments_history: This table is used for keeping track of the total time a fulfillment was in each state.


| fulfillment_id | state                   | time_in_state (sec) | 
|----------------|-------------------------|---------------------|
| 00000000       | ENTERING                | 400                 |
| 00000000       | TESTING                 | 3793                |
| 00000000       | LAUNCHING               | 324                 |
| 00000000       | WAITING                 | 823                 |
| 12345678       | WAITING                 | 324                 |

## Processing Strategy
In order to track each fulfillment along the proctoring process, they are sorted into states (ENTERING, WAITING, LAUNCHING, TESTING, and DONE). Each of these states is triggered by a certain event according to the table below (These state values and event triggers have been selected only to get a working prototype, actual states and events used could be different down the road):

| State     | Event trigger           |
|-----------|-------------------------|
|  ENTERING | Event::Welcome          |
| WAITING   | Event::LmiConnected     |
| LAUNCHING | Event::LaunchStart      |
| TESTING   | Event::LaunchEnd        |
| DONE      | Event::FulfillmentEnded |

When an event is received, it's logged to the `events` table in the datamart. The process then checks if the event is for a new fulfillment or for one that is already being tracked. In the case of a new fulfillment a row is inserted into the `current_fulfillments` table with the fulfillment id, the state it's in based on the event, and the time the event was triggered. In the case of an already tracked fulfillment, the row for that fulfillment either remains the same or is updated if the received event triggers a state change.

When an event triggers a state change, a row is inserted into the `fulfillment_history` table with the total time spent in that state (calculated by taking the difference in the most recent event's `created_at` time and the time the fulfillment started the previous state).

## Contents

This prototype consists of two python scripts:
* `emit_events.py`: Iterates through a CSV file of events from Archimedes and sends each one to a locally running RabbitMQ server.
* `receive_events.py`: Listens for those events and processes them as they come. Processing includes keeping track of which state fulfillments are in based on the events received (i.e. If the event `Event::LaunchStart` is received then that fulfillment is in the state `LAUNCHING`).

## Running it yourself

Not much set up is needed in order to get the prototype running locally. The first step is to install RabbitMQ and get a local instance running. If you're on Mac, this can be done using `homebrew`

```bash
brew update && brew install rabbitmq
```

After installing, you'll need to update the path and start a local server using the following commands

```bash
export PATH=$PATH:/usr/local/opt/rabbitmq/sbin
rabbitmq-server
```

While there is no need to know the inner workings of RabbitMQ in order to get the scripts up and running, if you want to understand the code you may need to read up through tutorial 3 (for python) [here](https://www.rabbitmq.com/getstarted.html "RabbitMQ Docs"). 

You'll need to ensure there is a running Postgres instance on your machine and that you're using a python version greater than 3.7.4.

To start the program, you should first start the `receive_events.py` script:

```bash
python3 receive_events.py
```
You should see some output along the lines of :
```
[x] Setting up database connection
[*] Waiting for events. To exit press CTRL+C
```
You can then start the the other script with the following command:
```
python3 emit_events.py data/events_2020-01-18.csv
```
You should see output spew with the following format:
```
...
[x] Sent Event::LaunchStart for fulfillment 15905113
[x] Sent Event::LmiDownload for fulfillment 15905261
[x] Sent Event::LaunchEnd for fulfillment 15905179
[x] Sent Event::FulfillmentEnded for fulfillment 15901307
[x] Sent Event::LaunchStart for fulfillment 15882537
...
```
And `receive_events.py` should now be outputting like so:
```
...
[x] Processing Event::LaunchStart for 15888487
[x] Processing Event::LmiDownload for 15723625
[x] Processing Event::LaunchStart for 15913305
[x] Processing Event::LaunchEnd for 15914216
[x] Processing Event::Welcome for 15806002
...
```




