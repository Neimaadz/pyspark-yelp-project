import ast
import json
import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

import os
from dotenv import load_dotenv
load_dotenv()

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import *
from py2neo import Graph
from py2neo.bulk import create_nodes


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


class App:
    def __init__(self, py2neo: Graph, spark: SparkSession):
        self.py2neo = py2neo
        self.spark = spark

    def getDifferentCategoryBusiness(self):
        spark = self.spark
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
        df4 = df3.withColumn("category", F.trim(df3.category)) # Remove white space
        df4.agg(F.countDistinct("category"))
        df5 = df4.groupBy("category").count().withColumnRenamed("count", "occurence")
        df5_sorted_desc = df5.sort(F.desc("occurence"))
        df5_sorted_desc.show()
        # Get specific category
        # df5.filter(F.col("category") == "Hostels").show()
        differentCategoryBusinessList = df5_sorted_desc.select("category", "occurence").collect()
        differentCategoryBusinesses = [differentCategoryBusiness for differentCategoryBusiness in differentCategoryBusinessList]
        
        self.py2neo.run("MATCH (n:DifferentCategoryBusiness) DELETE n")
        query = ("CREATE (p1:DifferentCategoryBusiness { category: $category, occurence_category: $category_occur })")
        for differentCategoryBusiness in differentCategoryBusinesses[:10]:
            params = {
                "category": differentCategoryBusiness.category,
                "category_occur": differentCategoryBusiness.occurence
                }
            self.py2neo.run(query, params)
        
    def getMostCommonUserName(self):
        spark = self.spark
        # Define data types
        json_schema = StructType([
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
        commonUserNames = [commonUserName for commonUserName in commonUserNameList]
        
        self.py2neo.run("MATCH (n:MostCommonUserName) DELETE n")
        query = ("CREATE (p1:MostCommonUserName { name: $name, occurence_name: $name_occur })")
        for commonUserName in commonUserNames[:10]:
            params = {
                "name": commonUserName.name,
                "name_occur": commonUserName.occurence
                }
            self.py2neo.run(query, params)
        
    def getMostUsedCommonWord(self):
        spark = self.spark
        json_schema = StructType([
            StructField('text', StringType())
        ])
        df = spark.read.option("multiLine", "true") \
            .json(dataset_path + '/yelp_academic_dataset_review/yelp_academic_dataset_review*.json', schema=json_schema)

        df2 = df.withColumn("text", F.regexp_replace(F.col("text"), "[^0-9a-zA-Z\s$]+", ""))
        df2 = df2.select(F.col("text"), F.split(F.col("text"),"\s+|,\s+").alias("textArray")).drop("text")
        df3 = df2.select(df2.textArray, F.posexplode(df2.textArray)).withColumnRenamed("col", "text")
        df4 = df3.withColumn("text", F.lower(F.trim(df3.text))) # To lower and remove white space
        df4.agg(F.countDistinct("text"))
        df5 = df4.groupBy("text").count().withColumnRenamed("count", "occurence")
        df5_sorted_desc = df5.sort(F.desc("occurence"))
        df5_sorted_desc.show()

    def getAveragePopularityByYearForGivenCity(self, city):
        spark = self.spark
        review_schema = StructType([
            StructField('business_id', StringType()),
            StructField('stars', FloatType()),
            StructField('date', DateType()),
        ])
        business_schema = StructType([
            StructField('business_id', StringType()),
            StructField('city', StringType()),
            StructField('review_count', IntegerType()),
        ])
        df_review = spark.read.option("multiLine", "true") \
            .json(dataset_path + '/yelp_academic_dataset_review/yelp_academic_dataset_review*.json', schema=review_schema)
        df_business = spark.read.option("multiLine", "true") \
            .json(dataset_path + '/yelp_academic_dataset_business/yelp_academic_dataset_business*.json', schema=business_schema)
        
        # Add new col named "year" by setting the year of col date
        df_review = df_review.withColumn("year", F.year("date"))
        # df_review.show()
        # Inner Join between DF review and DF business based on business_id
        df_review = df_review.join(df_business, df_review.business_id == df_business.business_id, "inner").drop(df_business.business_id)
        # df_review.show()
        # Group by needed cols and use agg(..) function to rename results of avg(..) by using alias(..)
        df2 = df_review.groupBy("business_id", "year", "review_count", "city").agg(F.avg("stars").alias("average_stars"))
        df2_sorted = df2.sort(F.col("review_count").desc(), F.col("year").asc())
        # df2_sorted.show()
        df3_filtered_by_city = df2_sorted.filter(F.col("city") == city)
        df3_filtered_by_city.printSchema()
        df3_filtered_by_city.show()
        # print(df2_sorted.count()) # result count = 977096
        # print(df3_filtered_by_city.count()) # result count = 43600
        popularityBusinessesList = df3_filtered_by_city.collect()
        popularityBusinesses = [popularityBusiness for popularityBusiness in popularityBusinessesList]
        popBusinesses = []
        popBusiness = {}
        avg_reviews = []
        avg_review = {}
        oldPopularityBusinessId = popularityBusinesses[0].business_id
        for popularityBusiness in popularityBusinesses:
            if oldPopularityBusinessId != popularityBusiness.business_id:
                popBusiness['avg_reviews'] = str(avg_reviews)
                popBusinesses.append(popBusiness)
                popBusiness = {}
                avg_reviews = []
                oldPopularityBusinessId = popularityBusiness.business_id
            
            popBusiness['business_id'] = popularityBusiness.business_id
            popBusiness['city'] = popularityBusiness.city
            popBusiness['review_count'] = popularityBusiness.review_count
            avg_review['year'] = popularityBusiness.year
            avg_review['average_stars'] = popularityBusiness.average_stars
            avg_reviews.append(avg_review)
            avg_review = {}
            
        self.py2neo.run("MATCH (n:AveragePopularityByYearForGivenCity) DELETE n")
        create_nodes(self.py2neo.auto(), popBusinesses, labels={"AveragePopularityByYearForGivenCity"})
        
        # result = self.py2neo.run("MATCH (n:AveragePopularityByYearForGivenCity WHERE n.review_count = 4554) RETURN n;").data()
        # print(result.business_id)
        # for res in result:
        #     print(res['business_id'])
        #     res._properties["avg_reviews"] = ast.literal_eval(res._properties["avg_reviews"])
        #     for r in res['avg_reviews']:
        #         print(r['year'])






if __name__ == "__main__":
    spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .getOrCreate()
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = os.getenv('NEO4J_URI')
    user = os.getenv('NEO4J_USERNAME')
    password = os.getenv('NEO4J_PASSWORD')
    py2neo = Graph(uri, auth=(user, password))
    app = App(py2neo, spark)
    
    # print("Get Most Common User Name")
    # app.getMostCommonUserName()
    # print("Get Different Category Business")
    # app.getDifferentCategoryBusiness()
    # print("Most Used Common word for a review")
    # app.getMostUsedCommonWord()
    print("Get Average Popularity By Year")
    app.getAveragePopularityByYearForGivenCity("New Orleans")
    
    # app.close()
