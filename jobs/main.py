import os
import uuid

from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import time
import random

LONDON_COORDINATES = {
    "latitude" : 51.5074,
    "longitude" : -0.1278
}

BIRMINGHAM_COORDINATES = {
    "latitude": 52.4862,
    "longitude": -1.8904
}

# Calculating the movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["latitude"] -
                      LONDON_COORDINATES["latitude"])/100

LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["longitude"] -
                      LONDON_COORDINATES["longitude"])/100


#Environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC','vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC','gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC','traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')


random.seed(42)
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30,60)) #update frequency
    return start_time

def generate_gps_data(device_id, timestamp, vehicle_type="private"):
    return {
        "id" : uuid.uuid4(),
        "devide_id" : device_id,
        "timestamp" : timestamp,
        "speed" : random.uniform(0,40),
        "direction" : "North-East",
        "VehicleType" : vehicle_type
    }

def generate_weather_data(device_id,timestamp,location):
    return {
        "id" : uuid.uuid4(),
        "devide_id" : device_id,
        "location" : location,
        "timestamp" : timestamp,
        "temparature" : random.uniform(-5,26),
        "weatherCondition" : random.choice(['Sunny','Rain','Cloudy','Snow']),
        "precipitation" : random.uniform(0,25),
        "windSpeed" : random.uniform(0,100),
        "humidity" : random.randint(0,100),
        "airQualityIndex" : random.uniform(0,500)
    }

def generate_emergency_incident_data(devide_id,timestamp,location):
    return{
        "id": uuid.uuid4(),
        "device_id": devide_id,
        "incident_id" : uuid.uuid4(),
        "type" : random.choice(["Accident", "Fire", "Medical","Polica","None"]),
        "timestamp" : timestamp,
        "location" : location,
        "status" : random.choice(["Active", "Resolved"]),
        "description"  : "Description of the incident"
    }
def simulate_vehice_movement():
    global start_location

    #move towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    #add more randomness to simulate actual road movement
    start_location['latitude'] += random.uniform(-0.0005,0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location



def generate_vehicle_data(device_id):
    location = simulate_vehice_movement()
    return {
        "id" : uuid.uuid4(),
        "device_id" : device_id,
        "timestamp" : get_next_time().isoformat(),
        'location' : (location["latitude"],location["longitude"]),
        "speed" : random.uniform(10,40),
        "direction" : 'North_East',
        "make" : "BMW",
        "model" : "C",
        "year" : 2024,
        "fuel_type" : "electric"
    }

def generate_traffic_camera_data(device_id,timestamp,location,camera_id,):
    return{
        "id" : uuid.uuid4(),
        "device_id" : device_id,
        "camera_id" : camera_id,
        "timestamp" : timestamp,
        "snapshot" : "Base64EncodedString",
        "location" : location
    }
def json_serializer(obj):
    if isinstance(obj,uuid.UUID):
        return str(obj)
    raise TypeError(f'object of type {obj.__class__.__name__} is not json serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery Failed : {err}')
    else:
        print(f'Message delivered to {msg.topic()} {msg.partition()}')

def produce_to_kafka(producer,topic,data):
    producer.produce(
        topic,
        key=str(data["id"]),
        value=json.dumps(data, default=json_serializer).encode("utf-8"),
        on_delivery=delivery_report
    )
    producer.flush()
def simulate_journey(producer , device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data["timestamp"])
        traffic_data = generate_traffic_camera_data(device_id,vehicle_data["timestamp"],vehicle_data["location"],"camera123",)
        weather_data = generate_weather_data(device_id,vehicle_data["timestamp"],vehicle_data["location"])
        emergency_incident_data = generate_emergency_incident_data(device_id,vehicle_data["timestamp"],vehicle_data["location"])

        if(vehicle_data["location"][0] >= BIRMINGHAM_COORDINATES["latitude"] and
        vehicle_data["location"][1] >= BIRMINGHAM_COORDINATES["longitude"]):
            print("vehicle has reached BIRMINGHAM , simulation ending....")
            break

        produce_to_kafka(producer,VEHICLE_TOPIC,vehicle_data)
        produce_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_to_kafka(producer, TRAFFIC_TOPIC, traffic_data)
        produce_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(3)


if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers' : KAFKA_BOOTSTRAP_SERVERS,
        'error_cb' : lambda err : print(f'Kafka error : {err}')
    }

    producer = SerializingProducer(producer_config)
    try:
        simulate_journey(producer, "Charan-Vehicle")
    except KeyboardInterrupt:
        print('Simulation ended by the User')
    except Exception as e:
        print(f'Unexpected Error occurred : {e}')


