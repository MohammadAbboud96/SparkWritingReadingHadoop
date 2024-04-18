from pyspark.sql import SparkSession
import time  
from pyspark.sql import functions as F

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Read CSV and Save to HDFS") \
        .getOrCreate()

    epochNow = int(time.time())

    csvName = 'salesRecord'
    # Define path to CSV file
    csv_file_path = "file:///data/"+csvName+".csv"

    # Read CSV file into DataFrame
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

    # Show DataFrame schema and content
    print("DataFrame schema:")
    print(df.printSchema())

    print("DataFrame content:")
    print(df.show())


    #Remove spaces in column names
    df = df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])

    hdfs_path = "hdfs://namenode:9000/sales/{}_{}.parquet".format(csvName,epochNow)

    # Repartition it by "Country" column before storing as parquet files in Hadoop
    df.write.option("header",True) \
            .partitionBy("Country") \
            .mode("overwrite") \
            .parquet(hdfs_path)

    # Read data from the specific partition
    print("Lebanon's sales Dataframe stored in Hadoop.")
    partition = "Country=Lebanon"

    df_load = spark.read.format("parquet") \
        .option("basePath", hdfs_path) \
        .option("path", f"{hdfs_path}/{partition}") \
        .load()
        
    print(df_load.show())

    # Stop Spark session
    spark.stop()
