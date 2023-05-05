import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

import psycopg2
conn = psycopg2.connect(database='yelp-project',
                        host='bigdata-project.postgres.database.azure.com',
                        user='cedalanavi',
                        password='C3daLAn@v!',
                        port='5432')

import findspark
findspark.init()
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

# /!\ ***** TO MODIFY ***** /!\
dataset_path = '/Users/Damien/Downloads/yelp_dataset_splitted'
# /!\ ***** TO MODIFY ***** /!\
    
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
    df5 = df4.groupBy("category").count().withColumnRenamed("count", "occurence")
    df5_sorted_desc = df5.sort(F.desc("occurence"))
    df5_sorted_desc.show()
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
    df2 = df.groupBy("name").count().withColumnRenamed("count", "occurence")
    df2_sorted_desc = df2.sort(F.desc("occurence"))
    df2_sorted_desc.show()
    
    commonUserNameList = df2_sorted_desc.select("name", "occurence").collect()
    # names = [commonUserName for commonUserName in commonUserNameList]
    # for name in names[:10]:
    #     print(commonUserName.name)
    #     print(commonUserName.occurence)

    # Convert list to tuples for executemany
    commonUserNames = [(commonUserName.name, commonUserName.occurence, ) for commonUserName in commonUserNameList[:10]]
    query_insert = "INSERT INTO users_name_occurence(name, occurence) VALUES(%s, %s)"
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users_name_occurence")
        # execute the INSERT statement
        # for commonUserName in commonUserNames[:10]:
        #     cursor.execute(query_insert, (commonUserName[0], commonUserName[1],))
        cursor.executemany(query_insert, (commonUserNames))
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    print('Finish')

def main():
    spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .getOrCreate()
    
    print("Get Most Common User Name")
    getMostCommonUserName(spark)
    # print("Get Different Category Business")
    # getDifferentCategoryBusiness(spark)

if __name__ == "__main__":
    main()

