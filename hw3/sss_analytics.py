# аналитика сообщений из Kafka с помощью Spark Structured Streaming

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, to_timestamp, from_json
from pyspark.sql.types import StructType, StringType

# структура JSON из kafka
message_schema = (
    StructType()
    .add("username", StringType())
    .add("timestamp", StringType())
)
# создаем sparksession
spark = (
    SparkSession.builder
    .appName("TelegramUserMessageStats")
    .getOrCreate()
)

# поток из kafka
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "telegram_messages")
    .load()
)

# value приходит как бинарные данные, приводим к строке и парсим JSON
parsed_stream = (
    raw_stream
    .select(from_json(col("value").cast("string"), message_schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", to_timestamp(col("timestamp")))
)

window_1min = (
    parsed_stream
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("username"),
    )
    .count()
    .orderBy(col("count").desc())
)

window_10min = (
    parsed_stream
    .groupBy(
        window(col("timestamp"), "10 minutes", "30 seconds"),
        col("username"),
    )
    .count()
    .orderBy(col("count").desc())
)

# вывод статистики в консоль
query_short = (
    window_1min.writeStream
    .outputMode("complete")      # вся агрегированная таблица
    .format("console")
    .option("truncate", False)   # строки не обрезаем
    .start()
)

query_long = (
    window_10min.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start()
)

query_short.awaitTermination()
query_long.awaitTermination()
