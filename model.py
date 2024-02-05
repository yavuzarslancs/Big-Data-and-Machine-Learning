from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import LinearRegression

def create_spark_session():
    return (
        SparkSession.builder.appName("AdvertisingPrediction")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )

def train_ml_model(static_data, training_data):
    static_data_with_label = static_data.withColumn("label", col("Product_Sold").cast(DoubleType()))

    feature_columns = ["TV","Billboards","Google_Ads","Social_Media","Influencer_Marketing","Affiliate_Marketing"]

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="label")  # Linear Regression

    pipeline = Pipeline(stages=[assembler, lr])
    model = pipeline.fit(static_data_with_label)

    return model


def main():
    spark = create_spark_session()

    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "bigdata"

    schema = (
        StructType()
        .add("TV", DoubleType())
        .add("Billboards", DoubleType())
        .add("Google_Ads", DoubleType())
        .add("Social_Media", DoubleType())
        .add("Influencer_Marketing", DoubleType())
        .add("Affiliate_Marketing", DoubleType())
        .add("Product_Sold", DoubleType())
    )

    static_data_path = "Advertising.csv"
    static_data = spark.read.csv(static_data_path, header=True, schema=schema)

    raw_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .load()
    )

    json_stream_df = raw_stream_df.selectExpr("CAST(value AS STRING) as value")

    parsed_stream_df = json_stream_df.select(
        from_json("value", schema).alias("data")
    ).select("data.*")

    processed_stream_df = parsed_stream_df.fillna(0)

    ml_model = train_ml_model(static_data, processed_stream_df)

    ml_result = ml_model.transform(processed_stream_df)

    query = (
        ml_result.writeStream.outputMode("append")
        .format("console")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()

if __name__== "__main__":
    main()
