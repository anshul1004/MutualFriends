"""
Author: Anshul Pardhi
Spark program to list top 10 businesses with best average rating
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark import SparkContext, SparkConf

# Boilerplate code to setup spark session
conf = SparkConf().setMaster("local").setAppName("YelpTop10")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Setup review dataframe from review.csv file
review = sc.textFile("review.csv").map(lambda line: line.split("::")).toDF()
review = review.select(review._1.alias('review_id'), review._2.alias('user_id'), review._3.alias('business_id'), review._4.cast('float').alias('stars'))

# Setup business dataframe from business.csv file
business = sc.textFile("business.csv").map(lambda line: line.split("::")).toDF()
business = business.select(business._1.alias('business_id'), business._2.alias('full_address'), business._3.alias('categories'))

# Select business_id, avg(stars) from review group by business_id order by avg(stars) desc 10
review = review.select('business_id', 'stars').groupBy('business_id').avg('stars').orderBy(desc('avg(stars)')).limit(10)

# Select distinct business_id, full_address, categories, avg(stars) from review r, business b where r.business_id = b.business_id
review = review.join(business, 'business_id').select('business_id', business['full_address'], business['categories'], 'avg(stars)').distinct()

# Generate the output folder
review.repartition(1).write.csv('out4.csv', header=True)
