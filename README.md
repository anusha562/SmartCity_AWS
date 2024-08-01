# Smart City Streaming Data Pipeline

## Overview

This project demonstrates a streaming data pipeline simulating vehicle movement and related data, publishing it to Kafka, and processing it with Apache Spark Structured Streaming. The processed data is then stored in AWS S3 in Parquet format. This setup can be used for various smart city applications, such as real-time traffic monitoring, weather reporting, and emergency incident management.

## Architecture

The pipeline consists of the following components:

- **Zookeeper**: Manages and coordinates the Kafka brokers.
- **Kafka Broker**: Handles the messaging system where producers send data to topics and consumers read from these topics.
- **Spark Master and Workers**: Handle the distributed data processing.
- **Kafka Producer Script**: Simulates data generation and sends it to Kafka topics.
- **PySpark Consumer Script**: Reads data from Kafka, processes it, and writes it to S3.

## Prerequisites

- Docker and Docker Compose
- Python 3.7 or higher
- AWS S3 bucket for data storage
- Kafka and Spark dependencies

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/smart-city-streaming.git
cd smart-city-streaming
```


### 2. Setup Environment Variables

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
VEHICLE_TOPIC=vehicle_data
GPS_TOPIC=gps_data
TRAFFIC_TOPIC=traffic_data
WEATHER_TOPIC=weather_data
EMERGENCY_TOPIC=emergency_data
AWS_ACCESS_KEY=your_aws_access_key
AWS_SECRET_KEY=your_aws_secret_key
```

### 3. Start Docker Services

```
docker-compose up -d
```

### 4.Run Kafka Producer Script

```
python kafka_producer.py
```

### 5. Run PySpark Consumer Script

```
python pyspark_consumer.py
```
