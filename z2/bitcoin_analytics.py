import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, avg, sum

def main():
    spark = SparkSession.builder \
        .appName("Bitcoin Analytics") \
        .getOrCreate()

    input_path = sys.argv[1]   
    output_path = sys.argv[2]  

    print(f"Reading data from: {input_path}")
    
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df_with_year = df.withColumn("year", year(col("datetime")))

    analytics_df = df_with_year.groupBy("year").agg(
        avg("close").alias("average_close_price"),
        sum("volume").alias("total_trading_volume")
    ).orderBy("year")

    print(f"Writing analytics results to: {output_path}")
    
    analytics_df.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()
