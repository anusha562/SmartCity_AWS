from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from config import configuration
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_kafka_topic(spark, topic, schema):
    return (spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'broker:29092')
            .option('subscribe', topic)
            .option('startingOffsets', 'earliest')
            .load()
            .selectExpr('CAST(value AS STRING)')
            .select(from_json(col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('timestamp', '2 minutes'))

def streamWriter(input_df: DataFrame, checkPointFolder, output):
    return (input_df.writeStream
            .format('parquet')
            .option('checkpointLocation', checkPointFolder)
            .option('path', output)
            .outputMode('append')
            .start())

def main():
    try:
        spark = SparkSession.builder.appName("SmartCityStreaming") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"
                    "org.apache.hadoop:hadoop-aws:3.3.1"
                    "com.amazonaws:aws-java-sdk:1.11.469") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY")) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()

        # Adjust the log level to minimize the console output
        spark.sparkContext.setLogLevel('WARN')

        vehicleSchema = StructType([
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("speed", DoubleType(), True),
            StructField("direction", StringType(), True),
            StructField("make", StringType(), True),
            StructField("model", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("fuel_type", StringType(), True)
        ])

        gpsSchema = StructType([
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("speed", DoubleType(), True),
            StructField("direction", StringType(), True),
            StructField("vehicleType", StringType(), True)
        ])

        trafficSchema = StructType([
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("cameraId", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("snapShot", StringType(), True)
        ])

        weatherSchema = StructType([
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("weatherCondition", StringType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("windSpeed", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("airQualityIndex", DoubleType(), True)
        ])

        emergencySchema = StructType([
            StructField("id", StringType(), True),
            StructField("deviceid", StringType(), True),
            StructField("incidentId", StringType(), True),
            StructField("type", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("status", StringType(), True),
            StructField("description", StringType(), True)
        ])

        vehicleDF = read_kafka_topic(spark, 'vehicle_data', vehicleSchema).alias('vehicle')
        gpsDF = read_kafka_topic(spark, 'gps_data', gpsSchema).alias('gps')
        trafficDF = read_kafka_topic(spark, 'traffic_data', trafficSchema).alias('traffic')
        weatherDF = read_kafka_topic(spark, 'weather_data', weatherSchema).alias('weather')
        emergencyDF = read_kafka_topic(spark, 'emergency_data', emergencySchema).alias('emergency')

        query1 = streamWriter(vehicleDF, "s3a://spark-streaming-data-charan/checkpoints/vehicle_data",
                              "s3a://spark-streaming-data-charan/data/vehicle_data")

        query2 = streamWriter(gpsDF, "s3a://spark-streaming-data-charan/checkpoints/gps_data",
                              "s3a://spark-streaming-data-charan/data/gps_data")

        query3 = streamWriter(trafficDF, "s3a://spark-streaming-data-charan/checkpoints/traffic_data",
                              "s3a://spark-streaming-data-charan/data/traffic_data")

        query4 = streamWriter(weatherDF, "s3a://spark-streaming-data-charan/checkpoints/weather_data",
                              "s3a://spark-streaming-data-charan/data/weather_data")

        query5 = streamWriter(emergencyDF, "s3a://spark-streaming-data-charan/checkpoints/emergency_data",
                              "s3a://spark-streaming-data-charan/data/emergency_data")

        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error(f"Error in streaming application: {e}")

if __name__ == "__main__":
    main()
