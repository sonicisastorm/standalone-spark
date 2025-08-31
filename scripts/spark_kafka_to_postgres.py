from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro

user_schema_json = open("schemas/user.avsc").read()

spark = (
    SparkSession.builder
    .appName("KafkaToPostgres")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.spark:spark-avro_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "users")
    .option("startingOffsets", "earliest")
    .load()
)

decoded = df.select(from_avro(df.value, user_schema_json).alias("data")).select("data.*")

def write_to_postgres(batch_df, batch_id):
    (batch_df.write
     .format("jdbc")
     .option("url", "jdbc:postgresql://postgres:5432/sparkdb")
     .option("dbtable", "user_events")
     .option("user", "sparkuser")
     .option("password", "sparkpass")
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .save())

query = (
    decoded.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/spark-checkpoint")
    .start()
)

query.awaitTermination()
