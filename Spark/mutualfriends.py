"""
Author: Anshul Pardhi
Spark program to calculate total mutual friends for all friend pairs
"""

from __future__ import print_function
from pyspark.sql import SparkSession


# The method takes the file content as input and splits the user id's from the friend list for each user
def generate_mutual_friends_list(inputs):

    input_size = len(inputs)
    i = 0
    key_list = []

    while i < input_size:
        val1 = inputs[i]  # User ID
        val2 = inputs[i + 1]  # Friend list
        if val2 != "":  # Proceed forward only if the user has at least 1 friend
            val2_list = val2.split(",")  # Split the friend list on the basis of comma
            for val in val2_list:
                # Make the smaller of the two id's appear first in the key
                if int(val1) < int(val):
                    key_str = val1 + ", " + val
                else:
                    key_str = val + ", " + val1
                key_list.append((key_str, val2_list))  # [("id1, id2", "friend list of id1 or id2")]
        i += 2
    return key_list


if __name__ == "__main__":

    # Create spark session
    spark = SparkSession.builder.appName("MutalFriends").getOrCreate()

    # Read file, convert to RDD, split on tabs and collect the result
    lines = spark.read.text('soc-LiveJournal1Adj.txt').rdd.map(lambda r: r[0]).\
        flatMap(lambda x: x.split("\t")).collect()

    # Split id's from friend list
    friend_list = generate_mutual_friends_list(lines)

    # Convert the list to RDD
    friend_list_rdd = spark.sparkContext.parallelize(friend_list)

    # Convert RDD to key value pairs, get the length of intersection of 2 generated lists to get total mutual friends
    output = friend_list_rdd.map(lambda x: (x[0], x[1])).reduceByKey(lambda x, y: len(set(x).intersection(y)))

    # Generate the output folder
    output.coalesce(1).saveAsTextFile("out1.txt")

    # Stop the spark session
    spark.stop()
