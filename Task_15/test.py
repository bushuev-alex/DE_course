# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#
# spark = SparkSession.builder.appName("DataFrame Example").getOrCreate()
# data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
#
# # Явное определение схемы
# schema = StructType([
#     StructField("Name", StringType(), True),
#     StructField("Value", IntegerType(), True)
# ])
#
# # Создание DataFrame с явной схемой
# df = spark.createDataFrame(data, schema)
# df.printSchema()




# from pyspark import SparkContext
#
# sc = SparkContext("local", "RDD Example")
# data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
# rdd = sc.parallelize(data)
#
# # Схема данных неявно определяется структурой кортежей
# print(rdd.collect())  # [('Alice', 1), ('Bob', 2), ('Cathy', 3)]



# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#
# spark = SparkSession.builder.appName("DataFrame Example").getOrCreate()
# data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
#
# # Явное определение схемы
# schema = StructType([
#     StructField("Name", StringType(), True),
#     StructField("Value", IntegerType(), True)
# ])
#
# # Создание DataFrame с явной схемой
# df = spark.createDataFrame(data, schema)
# df.printSchema()
#
# # Автоматическое определение схемы при чтении данных из CSV
# df_auto = spark.read.csv("/path/to/csv/file", header=True, inferSchema=True)
# df_auto.printSchema()



# import org.apache.spark.sql.SparkSession
# import org.apache.spark.sql.types._
#
# val spark = SparkSession.builder.appName("Dataset Example").getOrCreate()
# import spark.implicits._
#
# case class Person(name: String, age: Int)
# val data = Seq(Person("Alice", 1), Person("Bob", 2), Person("Cathy", 3))
# val ds = data.toDS()

"""ЧТЕНИЕ ИЗ POSTGRESQL"""


# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName("PySpark PostgreSQL Connection").config("spark.jars",
#                                                                              "postgresql-42.7.8.jar").getOrCreate()
# print("spark created")
#
# url = "jdbc:postgresql://localhost:5432/postgres"
# properties = {
#     "user": "",
#     "password": "",
#     "driver": "org.postgresql.Driver"
# }
#
# df = spark.read.jdbc(url=url, table="high_salary_employees", properties=properties)
#
# df.show()
#
# df.createOrReplaceTempView("my_table_view")
# spark.sql("SELECT * FROM my_table_view WHERE salary >= 65000").show()
#
# spark.stop()
# print("Done")







"""ЧТЕНИЕ И ЗАПИСЬ В POSTGRESQL"""

# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder \
#     .appName("PySpark PostgreSQL Connection") \
#     .config("spark.jars", "postgresql-42.7.8.jar") \
#     .getOrCreate()
#
# data = [
#     ("Alice", "Engineer", 75000, "2021-06-15"),
#     ("Bob", "Manager", 90000, "2020-05-01"),
#     ("Charlie", "HR", 60000, "2019-04-12"),
#     ("Diana", "Sales", 50000, "2018-01-25")
# ]
# columns = ["name", "position", "salary", "hire_date"]
#
# df = spark.createDataFrame(data, columns)
#
# df.show()
#
# filtered_df = df.filter(df.salary >= 60000)
#
# filtered_df.show()
#
#
# url = "jdbc:postgresql://localhost:5432/postgres"
# properties = {"user": "alexander",
#               "password": "",
#               "driver": "org.postgresql.Driver"
#               }
#
# filtered_df.write.jdbc(url=url,
#                        table="high_salary_employees",
#                        mode="overwrite",  # "overwrite" - если таблица уже существует, она будет перезаписана
#                        properties=properties
#                       )


"""ЧТЕНИЕ ИЗ CLICKHOUSE"""

# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder \
#     .appName("PySpark ClickHouse Connection") \
#     .config("spark.jars", "/home/alexander/PycharmProjects/NovaData/DE_course/Task_15/clickhouse-jdbc-0.4.6.jar") \
#     .getOrCreate()
#
# # Параметры подключения
# url = "jdbc:clickhouse://172.17.0.2:8123/default?jdbcCompliant=false"
# properties = {
#     "user": "default",  # Имя пользователя ClickHouse (по умолчанию "default")
#     "password": "",     # Пароль (по умолчанию пустой)
#     "driver": "com.clickhouse.jdbc.ClickHouseDriver"
# }
#
# df = spark.read.jdbc(url=url, table="employees", properties=properties)
# df.show()
#
# df.createOrReplaceTempView("employees_view")
#
# spark.sql("SELECT name FROM employees_view").show()
#
# spark.stop()



"""ЗАПИСЬ В CLICKHOUSE с clickhouse_driver из Python-a"""

# from clickhouse_driver import Client
# client = Client(host='127.17.0.2')
# create_table_query = """
# CREATE TABLE IF NOT EXISTS employees123
# (
#     name String,
#     position String,
#     salary Float64,
#     hire_date Date
# )
# ENGINE = MergeTree()
# ORDER BY name;
# """
# client.execute(create_table_query)
# print("Таблица employees123 успешно создана.")
#
#
#

"""ЗАПИСЬ В CLICKHOUSE"""

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("PySpark ClickHouse Connection") \
    .config("spark.jars", "/home/alexander/PycharmProjects/NovaData/DE_course/Task_15/clickhouse-jdbc-0.4.6.jar") \
    .getOrCreate()
url = "jdbc:clickhouse://172.17.0.2:8123/default"
properties = {
    "user": "default",  # Имя пользователя ClickHouse (по умолчанию "default")
    "password": "",     # Пароль (по умолчанию пустой)
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}
data = [
    ("Alice", "Engineer", 75000, "2021-06-15"),
    ("Bob", "Manager", 90000, "2020-05-01"),
    ("Charlie", "HR", 60000, "2019-04-12"),
    ("Diana", "Sales", 50000, "2018-01-25")
]
columns = ["name", "position", "salary", "hire_date"]
df = spark.createDataFrame(data, columns)
# df.write.jdbc(url=url, table="default.employees", mode="append", properties=properties)
df.write.jdbc(url=url, table="employees123", mode="append", properties=properties)
print("Данные успешно записаны в таблицу employees123")
spark.stop()
