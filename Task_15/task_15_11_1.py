from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql.functions import col, when, lit, mean, count, max, min, avg, year, month, dayofmonth, sum

spark = SparkSession.builder.appName("Weather").getOrCreate()
# data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]

# Явное определение схемы
schema = StructType([
    StructField("station_id", StringType(), False),
    StructField("date", DateType(), False),
    StructField("temperature", FloatType(), True),
    StructField("precipitation", FloatType(), True),
    StructField("wind_speed", FloatType(), True),
])
df = spark.read.csv("weather_data.csv", header=True, schema=schema)  # inferSchema=True



# Создание DataFrame с явной схемой
# df = spark.createDataFrame(data, schema)
# df.printSchema()
#
# # Автоматическое определение схемы при чтении данных из CSV
# df_auto = spark.read.csv("/path/to/csv/file", header=True, inferSchema=True)
# df.printSchema()
# df.fillna()
# df.show(50, False)
# print(df.dtypes)

df.na.fill({"temperature": df.agg(mean("temperature")).first()[0],
            "precipitation": df.agg(mean("precipitation")).first()[0],
            "wind_speed": df.agg(mean("wind_speed")).first()[0]}).show()

"""Найдите топ-5 самых жарких дней за все время наблюдений."""
df.select("date", "temperature").sort("temperature", ascending=False).show(5)

"""Найдите метеостанцию с наибольшим количеством осадков за последний год."""
# df.select("station_id", "date", "precipitation").sort("precipitation", ascending=False).show(1)
df.select(year("date").alias("year"), "station_id", "precipitation").groupBy("year", "station_id").agg(sum("precipitation").alias("sum")).sort("year", "sum", ascending=False).drop("year").show(1)

"""Подсчитайте среднюю температуру по месяцам за все время наблюдений."""
df.select(month("date").alias("month"), "temperature").groupBy("month").agg(avg("temperature").alias("avg")).sort("month").show()
