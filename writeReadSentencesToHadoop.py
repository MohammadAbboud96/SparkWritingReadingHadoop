from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Write to HDFS") \
        .master("spark://spark-master:7077")\
        .config("spark.executor.memory", "512m")\
        .getOrCreate()

    # Sample sentences
    sentences = [
        "Intelligencia Team.",
        "This is the first sentence to write into Hadoop using Spark.",
        "We will create a spark dataframe from this list of sentences.",
        "We will write data to HDFS using PySpark, the output path is the URL_of_Hadoop_NameNode followed by the path where to save the file."
    ]

    # Create a DataFrame from the sentences
    df = spark.createDataFrame([(sentence,) for sentence in sentences], ["sentence"])

    # Write DataFrame to HDFS in parquet format
    hdfs_output_path = "hdfs://namenode:9000/user/spark/sentences.parquet"
    df.write.mode("overwrite").parquet(hdfs_output_path)

    print("Sentences have been written to HDFS.")
    
    #Read the dataframe back from hdfs to confirm it was succesfully stored
    df_load = spark.read.parquet(hdfs_output_path)
    print("Sentences Dataframe read from Hadoop : ")
    print(df_load.show())

    # Stop the SparkSession
    spark.stop()
