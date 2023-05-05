import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

import findspark
findspark.init('C:/devtools/spark-3.4.0-bin-hadoop3/')
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Create a SparkConf object
conf = SparkConf().setAppName("MyApp") \
    .setMaster("local[*]") \
    .set("spark.driver.memory", "4g") \
    .set("spark.executor.memory", "4g") \
    # .set("spark.memory.offHeap.enabled","true") \
    # .set("spark.memory.offHeap.size","4g") \

dataset_path = 'C:/Users/Damien/Downloads/yelp_dataset/yelp_dataset_splitted'

def getDifferentCategoryBusiness(spark):
    json_schema = StructType([
        StructField('business_id', StringType()),
        StructField('name', StringType()),
        StructField('categories', StringType())
    ])
    df = spark.read.option("multiLine", "true") \
        .json(dataset_path + '/yelp_academic_dataset_business/yelp_academic_dataset_business*.json', schema=json_schema)
    df.show()
    # To convert categories dataset json from type string to array
    df2 = df.select(F.col("name"), F.col("business_id"), F.split(F.col("categories"),",").alias("categoryArray")).drop("categories")
    df2.show()
    # Creates a row for each element in the array and creates two columns 'pos' to hold the position of
    # the array element and the 'col' to hold the actual array value
    df3 = df2.select(F.col("name"), F.col("business_id"), df2.categoryArray, F.posexplode(df2.categoryArray)).withColumnRenamed("col", "category")
    df3.printSchema()
    df3.show()
    # Get the numbers of different categories
    df4 = df3.withColumn("Category", F.trim(df3.category)) # Remove white space
    df4.agg(F.countDistinct("category"))
    df5 = df4.groupBy("category").count().withColumnRenamed("count", "occurence_category")
    df5.sort(F.desc("occurence_category")).show()
    # Get specific category
    # df5.filter(F.col("category") == "Hostels").show()
    
def getMostCommonUserName(spark):
    # Define data types
    json_schema = StructType([
        StructField('user_id', StringType()),
        StructField('name', StringType())
    ])
    # Load data files
    df = spark.read.option("multiLine", "true") \
        .json(dataset_path + '/yelp_academic_dataset_user/yelp_academic_dataset_user*.json', schema=json_schema)
    df.agg(F.countDistinct("name"))
    df2 = df.groupBy("name").count().withColumnRenamed("count", "occurence_name")
    df2.sort(F.desc("occurence_name")).show()
    df2.printSchema()

def main():
    spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .getOrCreate()
    
    # print("Get Most Common User Name")
    # getMostCommonUserName(spark)
    print("Get Different Category Business")
    getDifferentCategoryBusiness(spark)
    
    sys.stdout = sys.__stdout__

if __name__ == "__main__":
    main()

