from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from classification import Classifier


class BigDataApi:

    def __init__(self):
        self.spark = (SparkSession
                      .builder
                      .appName('air-quality-api')
                      .getOrCreate())
        self.spark.sparkContext.setLogLevel('WARN')

        self.df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql:iot") \
            .option("dbtable", "air_data") \
            .option("user", "master") \
            .option("password", "1488") \
            .load()

    def filter(self, sensor_id=None, fromDate=None, toDate=None, fromTime=None, toTime=None):
        df = self.df

        df = df.withColumn('date_timestamp', to_timestamp(col('date'), 'dd/MM/yyyy')) \
            .withColumn('time_timestamp', to_timestamp(col('time'), 'HH:mm:ss'))

        if sensor_id:
            print('here')
            df = df.filter(df.sensor_id == sensor_id)

        if fromDate:
            df = df.filter(df['date_timestamp'] >= lit(fromDate))
        if toDate:
            df = df.filter(df['date_timestamp'] <= lit(toDate))
        if fromTime:
            df = df.filter(date_format(df['time_timestamp'], 'HH:mm:ss') >= lit(fromTime))
        if toTime:
            df = df.filter(date_format(df['time_timestamp'], 'HH:mm:ss') <= lit(toTime))

        return df

    def avg_hourly(self, df, column):
        return df.select(column, date_format('time_timestamp', 'HH:mm:ss').alias('time')) \
            .groupBy(hour('time').alias('hour')) \
            .avg(column) \
            .orderBy('hour')

    def avg_daily(self, df, column):
        return df.groupBy('date') \
            .avg(column) \
            .orderBy('date')

    def avg_monthly(self, df, column):
        return df.select(column, date_format('date_timestamp', 'yyyy MM').alias('date')) \
            .groupBy('date') \
            .avg(column) \
            .orderBy('date')

    def avg_annual(self, df, column):
        return df.groupBy(year('date_timestamp').alias('year')) \
            .avg(column) \
            .orderBy('year')

    def min_hourly(self, df, column):
        return df.select(column, date_format('time_timestamp', 'HH:mm:ss').alias('time')) \
            .groupBy(hour('time').alias('hour')) \
            .min(column) \
            .orderBy('hour')

    def min_daily(self, df, column):
        return df.groupBy('date') \
            .min(column) \
            .orderBy('date')

    def min_monthly(self, df, column):
        return df.select(column, date_format('date_timestamp', 'yyyy MM').alias('date')) \
            .groupBy('date') \
            .min(column) \
            .orderBy('date')

    def min_annual(self, df, column):
        return df.groupBy(year('date_timestamp').alias('year')) \
            .min(column) \
            .orderBy('year')

    def max_hourly(self, df, column):
        return df.select(column, date_format('time_timestamp', 'HH:mm:ss').alias('time')) \
            .groupBy(hour('time').alias('hour')) \
            .max(column) \
            .orderBy('hour')

    def max_daily(self, df, column):
        return df.groupBy('date') \
            .max(column) \
            .orderBy('date')

    def max_monthly(self, df, column):
        return df.select(column, date_format('date_timestamp', 'yyyy MM').alias('date')) \
            .groupBy('date') \
            .max(column) \
            .orderBy('date')

    def max_annual(self, df, column):
        return df.groupBy(year('date_timestamp').alias('year')) \
            .max(column) \
            .orderBy('year')

    def select_interests(self, df):
        columnsOfInterest = ['sensor_id', 'date', 'time', 'NO2', 'O3', 'PM10', 'PM25']

        return df.select(*columnsOfInterest)

    def to_csv(self, df, filename):
        df.coalesce(1) \
            .write \
            .mode('overwrite') \
            .option("header", True) \
            .csv(filename, sep=',')


def main(argv):
    api = BigDataApi()
    df = api.filter(fromTime='00:17:00')
    df = api.avg_daily(df, 'PM10')
    df.show()

if __name__ == '__main__':
    main(sys.argv)
