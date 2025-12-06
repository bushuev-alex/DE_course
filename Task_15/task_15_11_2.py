from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql.functions import col, when, lit, mean, count, max, min, avg, year, month, dayofmonth, sum

spark = SparkSession.builder.appName("Books").getOrCreate()
# data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]

# Явное определение схемы
schema_books = StructType([
    StructField("book_id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("author_id", IntegerType(), True),
    StructField("genre", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("publish_date", DateType(), True),
])
df_books = spark.read.csv("books.csv", header=True, schema=schema_books)  # inferSchema=True
# df_books.show(50, False)




schema_authors = StructType([
    StructField("author_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("birth_date", DateType(), True),
    StructField("country", StringType(), True),
])
df_authors = spark.read.csv("authors.csv", header=True, schema=schema_authors)
# df_authors.show(50, False)

df_ = df_books.join(df_authors, df_books.author_id == df_authors.author_id, "left")
df_.show()


"""Найдите топ-5 авторов, книги которых принесли наибольшую выручку."""
# df = df_.select("name", "price", df_books.author_id) \
#     .groupBy("author_id", "name").agg(sum("price").alias("total_revenue")).sort("total_revenue", ascending=False).show()



"""Найдите количество книг в каждом жанре."""
# df = df_.groupBy("genre").agg(count("book_id").alias("count")).sort("count", ascending=False).show()



"""Подсчитайте среднюю цену книг по каждому автору."""
# df = df_.groupBy(df_books.author_id, "name").agg(avg("price").alias("average_price")).sort("average_price", ascending=False).show()


"""Найдите книги, опубликованные после 2000 года, и отсортируйте их по цене."""
df = df_.filter(year("publish_date") >= 2000).sort("price", ascending=False).show()