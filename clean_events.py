import csv
import sys
import pandas as pd

valid_event_set = {'Event::Welcome', 
                   'Event::LmiConnected', 
                   'Event::LaunchStart', 
                   'Event::LaunchEnd', 
                   'Event::FulfillmentEnded'
                  }


if __name__ == "__main__":

    df = pd.read_csv(sys.argv[1])
    new_event_set = pd.DataFrame(columns=df.columns)

    fulfillments = pd.unique(df.fulfillment_id)
    print(f"Found {len(fulfillments)} fulfillments")

    for f_id in fulfillments:
        events_for_f = df[df['fulfillment_id'] == f_id]

        if set(events_for_f.type) == valid_event_set:
            new_event_set = new_event_set.append(events_for_f)

    fulfillments = pd.unique(new_event_set.fulfillment_id)
    print(f"Filtered down to {len(fulfillments)} fulfillments")





    # output new df to csv
    new_event_set.to_csv('cleaned_events.csv', index=False)


             

