from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

from classification import Classifier


class BigDataService:

    def __init__(self):
        self.host = 'localhost:9092'
        self.topic = 'air_quality-stream'
        self.schema = StructType()\
            .add("date", StringType())\
            .add("time", StringType())\
            .add("NO2", DoubleType())\
            .add("O3", DoubleType())\
            .add("PM10", DoubleType())\
            .add("PM25", DoubleType())\
            .add("quality", DoubleType())\

        self.spark = (SparkSession
                      .builder
                      .appName('air-quality')
                      .getOrCreate())
        self.spark.sparkContext.setLogLevel('WARN')

    def _preprocess(self):
        raw_df = (self.spark
                  .readStream
                  .format('kafka')
                  .option('kafka.bootstrap.servers', self.host)
                  .option("subscribe", self.topic)
                  .load())

        # Convert data from Kafka broker into String type
        processed_df = raw_df.selectExpr("CAST(value AS STRING) as value")

        # Parse json data
        parsed_df = processed_df.select(from_json(processed_df.value, self.schema).alias("air_data"))

        self.df = parsed_df.select(
            col("air_data.date").alias("date"),
            col("air_data.time").alias("time"),
            col("air_data.NO2").alias("NO2"),
            col("air_data.O3").alias("O3"),
            col("air_data.PM10").alias("PM10"),
            col("air_data.PM25").alias("PM25"),
            col("air_data.quality").alias("quality"))

    def stream(self, csv=None, mongo=False):
        self._preprocess()

        query = self.to_console()

        if csv:
            query = self.to_csv('data/data.csv')

        if mongo:
            query = self.to_mongo()

        query.awaitTermination()

    def stream_predictions(self, csv=None, mongo=False):
        self._preprocess()
        self._train()

        query = self.to_console(func=self._get_prediction)

        if csv:
            query = self.to_csv('data/data.csv', func=self._get_prediction)

        if mongo:
            query = self.to_mongo(func=self._get_prediction)

        query.awaitTermination()

    def read_from_db(self):
        df = self.spark.read.format("mongo")\
            .option("uri", "mongodb://127.0.0.1/IoT.AirData").load()

        return df

    def filter(self, fromDate=None, toDate=None, fromTime=None, toTime=None):
        df = self.read_from_db()
        df.withColumn('date_timestamp', to_timestamp(col('date'), 'dd/MM/yyyy'))
        df.withColumn('time_timestamp', to_timestamp(col('time'), 'HH:mm:ss'))

        if fromDate:
            df = df.filter(df('date_timestamp').gt(lit(fromDate)))

        if toDate:
            df = df.filter(df('date_timestamp').lt(lit(toDate)))

        if fromTime:
            df = df.filter(df('time_timestamp').gt(lit(fromTime)))

        if toTime:
            df = df.filter(df('time_timestamp').lt(lit(toTime)))

        return df

    def avg_hourly(self, df, column):
        return df.groupBy(hour('time_timestamp').alias('year')) \
            .avg(column) \
            .orderBy('year')

    def avg_daily(self, df, column):
        return df.groupBy('date')\
            .avg(column)\
            .orderBy('date')

    def avg_monthly(self, df, column):
        return df.select('PM10', date_format('date_timestamp', 'yyyy MM').alias('date'))\
            .groupBy('date')\
            .avg(column)\
            .orderBy('date')

    def avg_annual(self, df, column):
        return df.groupBy(year('date_timestamp').alias('year'))\
            .avg('PM10')\
            .orderBy('year')

    def select_interests(self, df):
        columnsOfInterest = ['sensor_id', 'date', 'time', 'NO2', 'O3', 'PM10', 'PM25']

        return df.select(*columnsOfInterest)

    def _train(self):
        self.classifier = Classifier()
        self.classifier.run()

    def _get_prediction(self, df, batchId):
        print('here')
        predictions = self.classifier.predict(df)
        print('Batch: ' + batchId)
        predictions.show(5)

    def to_console(self, func=None):
        console = self.select_interests(self.df)
        print("console schema")
        console.printSchema()

        if func:
            console = console.writeStream.foreachBatch(func)\
                .format('console') \
                .option("checkpointLocation", "checkpoint/") \
                .queryName('console-output')
        else:
            console = console.writeStream.format('console')\
                .option("checkpointLocation", "checkpoint/")\
                .queryName('console-output')

        return console.start()

    def to_mongo(self, func=None):
        db = self.df

        if func:
            db.writeStream.foreachBatch(func)\
                .format('mongodb') \
                .option("checkpointLocation", "checkpoint/") \
                .option("spark.mongodb.connection.uri", 'mongodb://mongodb0.example.com:27017') \
                .option("spark.mongodb.database", 'IoT') \
                .option("spark.mongodb.collection", 'AirData') \
                .outputMode("append")
        else:
            db.writeStream \
                .format('mongodb') \
                .option("checkpointLocation", "checkpoint/") \
                .option("spark.mongodb.connection.uri", 'mongodb://mongodb0.example.com:27017') \
                .option("spark.mongodb.database", 'IoT') \
                .option("spark.mongodb.collection", 'AirData') \
                .outputMode("append")

        return db.start()

    def to_csv(self, path, func=None):
        csv = self.df

        if func:
            csv.writeStream.foreachBatch(func)\
                .format('csv') \
                .option("checkpointLocation", "checkpoint/") \
                .option("path", path) \
                .outputMode("append")
        else:
            csv.writeStream.format('csv') \
                .option("checkpointLocation", "checkpoint/") \
                .option("path", path) \
                .outputMode("append")

        return csv.start()
