from kafka import KafkaConsumer
import json
import pandas as pd
from dynamic_pricing_model import dynamic_price_predict


def create_consumer(bootstrap_servers=None, topic='ride_request'): 
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers or ['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='ride_request_consumer_group'
    )


def process_event(event):
    df = pd.DataFrame([{
        'origin_lat': event['origin'][0],
        'origin_lon': event['origin'][1],
        'destination_lat': event['destination'][0],
        'destination_lon': event['destination'][1],
        'expected_ride_duration': event['expected_ride_duration'],
        'time_of_booking': event['time_of_booking'],
        'location_category': event['location_category'],
        'customer_loyalty_status': event['customer_loyalty_status'],
        'number_of_past_rides': event['number_of_past_rides'],
        'average_ratings': event['average_ratings'],
        'vehicle_type': event['vehicle_type'],
        'Number_of_Riders': event['Number_of_Riders'],
        'Number_of_Drivers': event['Number_of_Drivers']
    }])


    price = dynamic_price_predict(df)
    print(f"-> Ride {event['ride_id']} predicted price: {price[0]:.2f}")


if __name__ == '__main__':
    consumer = create_consumer()
    for msg in consumer:
        process_event(msg.value)