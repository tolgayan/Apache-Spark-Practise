from pyspark.sql import SparkSession

def init_spark():
    spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
    sc = spark.sparkContext
    return spark,sc

if __name__ == '__main__':
    # init SparkSession
    spark,sc = init_spark()
    # generate a dummy DataFrame
    myRange = spark.range(1000).toDF("number")
    # find all even numbers in the DataFrame
    divisBy2 = myRange.where("number % 2 = 0")
    print(divisBy2.count())