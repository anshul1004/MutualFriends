"""
Author: Anshul Pardhi
Spark program to list users who reviewed businesses located in Stanford
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

# Boilerplate code to setup spark session
conf = SparkConf().setMaster("local").setAppName("YelpStanford")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Setup review dataframe from review.csv file
review = sc.textFile("review.csv").map(lambda line: line.split("::")).toDF()
review = review.select(review._1.alias('review_id'), review._2.alias('user_id'), review._3.alias('business_id'), review._4.alias('stars'))

# Setup business dataframe from business.csv file
business = sc.textFile("business.csv").map(lambda line: line.split("::")).toDF()
business = business.select(business._1.alias('business_id'), business._2.alias('full_address'), business._3.alias('categories'))


# Select business_id from business where full_address contains Stanford
business_stanford = business.where(business['full_address'].contains('Stanford')).select('business_id')

# Select user_id, stars from review r, business_stanford b where r.business_id = b.business_id
output = review.join(business_stanford, 'business_id').select('user_id', 'stars')

# Generate the output folder
output.repartition(1).write.csv('out3.csv', header=True)
