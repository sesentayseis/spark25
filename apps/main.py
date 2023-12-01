from pyspark.sql import SparkSession

def init_spark():
    spark = SparkSession.builder \
        .appName("trip-app") \
        .config("spark.jars", "/opt/spark-apps/postgresql-42.2.22.jar") \
        .getOrCreate()
    sc = spark.sparkContext
    return spark, sc

def main():
    url = "jdbc:postgresql://demo-database:5432/movilens"
    properties = {
        "user": "postgres",
        "password": "casa1234",
        "driver": "org.postgresql.Driver"
    }
    file = "/opt/spark-data/ratings.csv"
    spark, sc = init_spark()

    df = spark.read.load(file, format="csv", inferSchema=True, sep=",", header=True)

    # Filter invalid coordinates
    df.write \
        .jdbc(url=url, table="movilens", mode='append', properties=properties)

if __name__ == '__main__':
    main()
