import click
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


class Consumer:
    def __init__(self):
        self.host = 'localhost:9092'
        self.topic = 'air_quality_1'
        self.schema = StructType()\
            .add("sensor_id", StringType())\
            .add("date", StringType())\
            .add("time", StringType())\
            .add("NO2", DoubleType())\
            .add("O3", DoubleType())\
            .add("PM10", DoubleType())\
            .add("PM25", DoubleType())\
            .add("quality", DoubleType())\

        self.spark = (SparkSession
                      .builder
                      .appName('air-quality-stream')
                      .getOrCreate())
        self.spark.sparkContext.setLogLevel('WARN')

        self._preprocess()

    def _preprocess(self):
        raw_df = (self.spark
                  .readStream
                  .format('kafka')
                  .option('kafka.bootstrap.servers', self.host)
                  .option("subscribe", self.topic)
                  .option("startingOffsets", "latest")
                  .load())

        # Convert data from Kafka broker into String type
        processed_df = raw_df.selectExpr("CAST(value AS STRING) as value")

        # Parse json data
        parsed_df = processed_df.select(from_json(processed_df.value, self.schema).alias("air_data"))

        self.df = parsed_df.select(
            col("air_data.sensor_id").alias("sensor_id"),
            col("air_data.date").alias("date"),
            col("air_data.time").alias("time"),
            col("air_data.NO2").alias("NO2"),
            col("air_data.O3").alias("O3"),
            col("air_data.PM10").alias("PM10"),
            col("air_data.PM25").alias("PM25"),
            col("air_data.quality").alias("quality"))

        self.columnsOfInterest = ['sensor_id', 'date', 'time', 'NO2', 'O3', 'PM10', 'PM25']

    def stream(self, csv=None, mongo=False):
        self._preprocess()

        query = self.to_console()

        if csv:
            query = self.to_csv()

        if mongo:
            query = self.to_db()

        query.awaitTermination()

    def to_console(self):
        console = self.df.select(*self.columnsOfInterest)
        print("console schema")
        console.printSchema()

        console = console.writeStream.format('console') \
            .option("checkpointLocation", "checkpoint/") \
            .queryName('console-output')

        return console.start()

    def save_to_db(self, df, batch):
        df.write \
            .format("jdbc") \
            .mode('append') \
            .option("url", "jdbc:postgresql:iot") \
            .option("dbtable", "air_data") \
            .option("user", "master") \
            .option("password", "1488") \
            .save()

    def to_db(self):
        db = self.df.select(*self.columnsOfInterest)

        db = db.writeStream \
            .foreachBatch(self.save_to_db) \
            .option("driver", "org.postgresql.Driver") \
            .option("checkpointLocation", "checkpoint/") \
            .trigger(processingTime='10 seconds')

        return db.start()

    def to_csv(self):
        csv = self.df

        csv = csv.writeStream.format('csv') \
            .trigger(processingTime="10 seconds") \
            .option("checkpointLocation", "checkpoint/") \
            .option("path", 'data/write/') \
            .outputMode("append")\
            .queryName('csv-output')

        return csv.start()


@click.command()
@click.option('--csv', is_flag=True, required=False)
@click.option('--db', is_flag=True, required=False)
def main(csv, db):
    consumer = Consumer()
    consumer.stream(csv, db)


if __name__ == '__main__':
    main()