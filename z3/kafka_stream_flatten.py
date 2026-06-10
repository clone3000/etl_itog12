#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

def main():
    output_path = sys.argv   
    checkpoint_path = sys.argv 

    spark = SparkSession.builder \
        .appName("Kafka JSON Streaming Flatten") \
        .getOrCreate()

    json_schema = StructType([
        StructField("application_id", StringType(), True),
        StructField("customer", StructType([
            StructField("customer_id", StringType(), True),
            StructField("region", StringType(), True)
        ]), True),
        StructField("loan", StructType([
            StructField("amount", IntegerType(), True),
            StructField("term_months", IntegerType(), True)
        ]), True),
        StructField("scoring", StructType([
            StructField("score", IntegerType(), True),
            StructField("risk_level", StringType(), True)
        ]), True),
        StructField("documents", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("status", StringType(), True)
        ])), True),
        StructField("decision_status", StringType(), True),
        StructField("submitted_at", StringType(), True)
    ])

    query = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1a-muv59hvlsm1a6aoa.mdb.yandexcloud.net:9091") \
        .option("subscribe", "loan_applications") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
              "org.apache.kafka.common.security.scram.ScramLoginModule required "
              "username=kafka_user "
              "password=kafka_user123 "
              ";") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as raw_value") \
        .where(col("raw_value").isNotNull()) \
        .writeStream \
        .trigger(once=True) \
        .queryName("received_messages") \
        .format("memory") \
        .option("checkpointLocation", checkpoint_path) \
        .start()

    query.awaitTermination()

    raw_df = spark.sql("select raw_value from received_messages")

    parsed_df = raw_df.withColumn("json_data", from_json(col("raw_value"), json_schema))

    flattened_df = parsed_df.select(
        col("json_data.application_id").alias("application_id"),
        col("json_data.customer.customer_id").alias("customer_id"),
        col("json_data.customer.region").alias("region_code"),
        col("json_data.loan.amount").alias("requested_amount"),
        col("json_data.loan.term_months").alias("term_months"),
        col("json_data.scoring.score").alias("credit_score"),
        col("json_data.scoring.risk_level").alias("risk_level"),
        explode(col("json_data.documents")).alias("doc"), 
        col("json_data.decision_status").alias("decision_status"),
        col("json_data.submitted_at").alias("submitted_at")
    ).select(
        "*",
        col("doc.type").alias("document_type"),
        col("doc.status").alias("document_status")
    ).drop("doc")

    flattened_df.write.mode("overwrite").format("parquet").save(output_path)
    spark.stop()

if __name__ == "__main__":
    main()
