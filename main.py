def main():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as f

    from schema_example3 import schema

    spark = (SparkSession
             .builder
             .appName('streaming-kafka')
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')

    source = (spark
              .readStream
              .format('kafka')
              .option('kafka.bootstrap.servers', 'localhost:9092')
              .option('subscribe', 'test')
              .load())

    df = (source
          .selectExpr('CAST(value AS STRING)', 'offset'))

    df = (df
          .select(f.from_json('value', schema).alias('data')))
    print("df schema")
    df.printSchema()

    console = df.select('data.*')
    print("console schema")
    console.printSchema()

    console = (console
               .writeStream
               .format('console')
               .queryName('console-output'))

    console.start().awaitTermination()


if __name__ == '__main__':
    main()
    
