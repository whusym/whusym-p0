# MIT License
#
# Copyright (c) 2018 Data Science Practicum
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
from __future__ import print_function
import json, sys, os
from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext

if __name__ == "__main__":
    """
    Usage: calculate the total word counts for top words among 8 books in the Gutenberg Project
    """
    if len(sys.argv) != 2 or not int(sys.argv[1]):
        print("Please only add one integer as the argument")
        exit(-1)

    #initiate a spark session
    spark = SparkSession\
        .builder\
        .appName("WordCounter")\
        .getOrCreate()

    print (sys.argv)
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    data_path = os.path.join(script_dir, 'data/')
    sw_path = os.path.join(script_dir, 'stopwords.txt')
    sw = spark.sparkContext.textFile(sw_path)
    swlist = spark.sparkContext.broadcast(sw.collect())
    books = spark.sparkContext.wholeTextFiles(data_path) # Books (*.txt files) are in the /data folder
    counts = books.map(lambda x: x[1].lower()).flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
    new_count = counts.filter(lambda x: x[0] not in swlist.value).collect() #get rid of lower words with lower than 2 appearances
    res = sorted(new_count, key=lambda x:x[1], reverse = True) #sort the list as the result
    res = res[0:int(sys.argv[1])] # get the top x number of words. x provided by sys.argv[1]
    res_file = os.path.join(script_dir, 's2.json')
    with open(res_file, 'w') as file:
        json.dump(dict(res), file)
