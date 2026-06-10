#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, expr, to_json, struct

def main():
    spark = SparkSession.builder \
        .appName("Spark Kafka Writer JSON") \
        .getOrCreate()

    raw_df = spark.range(0, 60000)

    payload_df = raw_df.select(
        struct(
            concat(lit("loan_2026_"), col("id")).alias("application_id"),
            struct(
                concat(lit("cust_"), expr("cast(rand()*900 + 100 as int)")).alias("customer_id"),
                expr("case when rand() < 0.25 then 'DE-HE' when rand() < 0.5 then 'DE-BY' when rand() < 0.75 then 'DE-BE' else 'DE-NW' end").alias("region")
            ).alias("customer"),
            struct(
                expr("cast(rand()*45000 + 5000 as int)").alias("amount"),
                lit(36).alias("term_months")
            ).alias("loan"),
            struct(
                expr("cast(rand()*350 + 500 as int)").alias("score"),
                expr("case when rand() < 0.33 then 'low' when rand() < 0.66 then 'medium' else 'high' end").alias("risk_level")
            ).alias("scoring"),
            expr("array(struct('passport' as type, 'verified' as status))").alias("documents"),
            expr("case when rand() < 0.33 then 'approved' when rand() < 0.66 then 'manual_review' else 'declined' end").alias("decision_status"),
            lit("2026-05-01T10:15:11Z").alias("submitted_at")
        ).alias("payload")
    )

    df_to_kafka = payload_df.select(to_json(col("payload")).alias('value'))

    df_to_kafka.write.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1a-muv59hvlsm1a6aoa.mdb.yandexcloud.net:9091") \
        .option("topic", "loan_applications") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
              "org.apache.kafka.common.security.scram.ScramLoginModule required "
              "username=kafka_user "
              "password=kafka_user123 "
              ";") \
        .save()

    spark.stop()

if __name__ == "__main__":
    main()
