from pyspark.sql import types as t

schema = t.StructType(
    [
        t.StructField('number', t.IntegerType(), True),
    ],
)
