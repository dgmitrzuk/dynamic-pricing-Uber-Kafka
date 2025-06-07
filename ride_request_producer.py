from kafka import KafkaProducer
import json
import random
import uuid
import time


def create_producer(bootstrap_servers=None):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers or ['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def random_origin_coord():
    lat = random.uniform(52.0977778, 52.3680556)
    lon = random.uniform(20.8516667, 21.2711111)
    return (lat, lon)


def random_destination_coord(origin, radius_km=30):
    max_deg = radius_km / 111
    return (
        origin[0] + random.uniform(-max_deg, max_deg),
        origin[1] + random.uniform(-max_deg, max_deg)
    )


def random_drivers_riders():
    return {
        'Number_of_Riders': random.randint(50, 200),
        'Number_of_Drivers': random.randint(30, 150),
    }


def expected_ride_duration(origin, destination, speed_kmh=65):
    from geopy.distance import geodesic
    distance_km = geodesic(origin, destination).km
    return distance_km / speed_kmh


def send_ride_request(producer):
    ride_id = str(uuid.uuid4())
    origin = random_origin_coord()
    destination = random_destination_coord(origin)
    event = {
        'ride_id': ride_id,
        'origin': origin,
        'destination': destination,
        'expected_ride_duration': expected_ride_duration(origin, destination),
        'time_of_booking': random.choice(['Morning', 'Afternoon', 'Evening', 'Night']),
        'location_category': random.choice(['Urban', 'Suburban', 'Rural']),
        'customer_loyalty_status': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
        'number_of_past_rides': random.randint(0, 200),
        'average_ratings': round(random.uniform(1, 5), 2),
        'vehicle_type': random.choice(['Economy', 'Premium', 'SUV', 'Luxury']),
        **random_drivers_riders()
    }
    producer.send('ride_request', value=event)
    producer.flush()
    print(f"-> Sent ride request: {ride_id}")


if __name__ == '__main__':
    prod = create_producer()
    while True:
        send_ride_request(prod)
        time.sleep(1)