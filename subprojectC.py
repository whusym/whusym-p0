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
from collections import OrderedDict

if __name__ == "__main__":
    """
    Usage: calculate the total word counts for top words among 8 books in the Gutenberg Project
    """
    if len(sys.argv) != 4 or not int(sys.argv[1]):
        print("Please set one integer as the first argument and a punctuation file as the second argument")
        exit(-1)

    #initiate a spark session
    spark = SparkSession\
        .builder\
        .appName("WordCounterWithoutPunc")\
        .getOrCreate()

    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    data_path = os.path.join(script_dir, 'data/')
    books = spark.sparkContext.wholeTextFiles(data_path) # Books (*.txt files) are in the /data folder

    #stopwords
    sw_path = os.path.join(script_dir, str(sys.argv[2]))
    sw = spark.sparkContext.textFile(sw_path)
    swlist = spark.sparkContext.broadcast(sw.collect())

    #punctuations
    punc_path = os.path.join(script_dir, str(sys.argv[3]))
    with open(punc_path, 'r') as f:
        punc = f.read()

    # # punc = spark.sparkContext.textFile(punc_path, use_unicode=False)
    # punc = spark.sparkContext.parallelize(punc)
    # print (punc.collect())
    punc_list = spark.sparkContext.broadcast(punc)
    print (punc_list.value)

    def split(x):
        x = x.splitlines()
        x = ' '.join(x)
        # x = x.split(' ')
        # a = []
        return x

    def strip_front(x):
        try:
            while x[0] in punc_list.value: # change it to while
                x = x[1:]
        except IndexError:
            return x
        return x

    def strip_end(x):
        try:
            while x[-1] in punc_list.value:  # change it to while
                x = x[:-1]
        except IndexError:
            return x
        return x




    counts = books.map(lambda x: x[1].lower()).map(split).flatMap(lambda x: x.split(' ')).filter(lambda x: len(x) > 1).map(strip_end).map(strip_front)
    new_count = counts.map(lambda x: (x, 1)).reduceByKey(add).filter(lambda x: x[0] != "").filter(lambda x: x[0] not in swlist.value).collect() #remove stop words
    res = sorted(new_count, key=lambda x:x[1], reverse = True) #sort the list as the result
    res = res[0:1+int(sys.argv[1])] # get the top x number of words. x provided by sys.argv[1]
    res_file = os.path.join(script_dir, 'sp3.json')
    with open(res_file, 'w') as file:
        json.dump(OrderedDict(res), file)
