"""
Author: Anshul Pardhi
Spark program to calculate top-10 mutual friend pairs and their basic information
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
                    key_str = val1 + "," + val
                else:
                    key_str = val + "," + val1
                key_list.append((key_str, val2_list))  # [("id1,id2", "friend list of id1 or id2")]
        i += 2
    return key_list


# The method takes the userdata file as input and extracts the first name, last name & address for each user id
def generate_user_info(inputs):
    input_size = len(inputs)
    i = 0
    key_dict = dict()

    while i < input_size:
        key_dict[inputs[i]] = inputs[i + 1] + "\t" + inputs[i + 2] + "\t" + inputs[i + 3]
        i += 10
    return key_dict  # (user_id, fname+lname+address)


if __name__ == "__main__":

    # Create spark session
    spark = SparkSession.builder.appName("MutalFriendsTop10").getOrCreate()

    # Read file, convert to RDD, split on tabs and collect the result
    lines = spark.read.text('soc-LiveJournal1Adj.txt').rdd.map(lambda r: r[0]). \
        flatMap(lambda d: d.split("\t")).collect()

    # Split id's from friend list
    friend_list = generate_mutual_friends_list(lines)

    # Convert the list to RDD
    friend_list_rdd = spark.sparkContext.parallelize(friend_list)

    # Convert RDD to key value pairs, get the length of intersection of 2 generated lists to get total mutual friends
    # and then extract the top 10 mutual friend pairs
    output = friend_list_rdd.map(lambda x: (x[0], x[1])).reduceByKey(lambda x, y: len(set(x).intersection(y))) \
        .takeOrdered(10, key=lambda x: -x[1])

    # Convert to RDD
    output_rdd = spark.sparkContext.parallelize(output)

    # Read user data file, convert to RDD, split on commas and collect the result
    user_info = spark.read.text('userdata.txt').rdd.map(lambda r: r[0]). \
        flatMap(lambda x: x.split(",")).collect()

    # Map user id to the required user info
    user_info_dict = generate_user_info(user_info)

    # Collect the top 10 mutual friends
    top_10_list = output_rdd.collect()

    top_10_result = []
    for key in top_10_list:
        result = ""
        arr = key[0].split(",")  # Split the top 10 pairs' key

        # Generate output, concatenating mutual friend count along with both the users' info
        result += str(key[1]) + "\t" + user_info_dict[arr[0]] + "\t" + user_info_dict[arr[1]]
        top_10_result.append(result)

    # Convert the list to RDD
    top_10_result_rdd = spark.sparkContext.parallelize(top_10_result)

    # Generate the output folder
    top_10_result_rdd.coalesce(1).saveAsTextFile("out2.txt")

    # Stop the spark session
    spark.stop()
