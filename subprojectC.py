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

    #Reading files from the /data folder under the python app
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    data_path = os.path.join(script_dir, 'data/')
    books = spark.sparkContext.wholeTextFiles(data_path) # Books (*.txt files) are in the /data folder

    #Reading stopwords from stopwords list
    sw_path = os.path.join(script_dir, str(sys.argv[2]))
    sw = spark.sparkContext.textFile(sw_path)
    swlist = spark.sparkContext.broadcast(sw.collect())

    #Punctuations used as broadcasted values
    punc_path = os.path.join(script_dir, str(sys.argv[3]))
    with open(punc_path, 'r') as f:
        punc = f.read()
    punc_list = spark.sparkContext.broadcast(punc)


    def split(x):
        '''
        Split characters in a documents (including removing newline markers).
        '''
        x = x.splitlines()
        x = ' '.join(x)
        return x

    def strip_end(x):
        return (x[0][:-1], x[1]) if x[0][-1] in punc_list.value else (x[0], x[1])

    def strip_front(x):
        return (x[0][1:], x[1]) if x[0][0] in punc_list.value else (x[0], x[1])

    #count words in RDD
    counts = books.map(lambda x: x[1].lower()).map(split).flatMap(lambda x: x.split(' ')).filter(lambda x: len(x) > 1)    #count words and remove the keys containing punctuation markers in the beginning or end
    new_count = counts.map(lambda x: (x, 1)).reduceByKey(add).filter(lambda x: x[0] != "").filter(lambda x: x[0] not in swlist.value).map(strip_end).map(strip_front).collect() #remove stop words
    res = sorted(new_count, key=lambda x:x[1], reverse = True) #sort the list based on the count
    res = res[0:int(sys.argv[1])]  # get the top x number of words. x provided by sys.argv[1]


    #write in a json file as the output
    res_file = os.path.join(script_dir, 'sp3.json')
    with open(res_file, 'w') as file:
        json.dump(OrderedDict(res), file)
