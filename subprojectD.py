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
    if len(sys.argv) != 3 or not int(sys.argv[1]):
        print("Please set one integer as the first argument and a punctuation file as the second argument")
        exit(-1)

    #initiate a spark session
    spark = SparkSession\
        .builder\
        .appName("tfidf")\
        .getOrCreate()

    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    data_path = os.path.join(script_dir, 'data/')
    books = spark.sparkContext.wholeTextFiles(data_path) # Books (*.txt files) are in the /data folder

    #stopwords
    sw_path = os.path.join(script_dir, 'stopwords.txt')
    sw = spark.sparkContext.textFile(sw_path)
    swlist = spark.sparkContext.broadcast(sw.collect())

    #punctuations
    punc_path = os.path.join(script_dir, str(sys.argv[2]))
    with open(punc_path, 'r') as f:
        punc = f.read()
    print (punc)
    # # punc = spark.sparkContext.textFile(punc_path, use_unicode=False)
    # punc = spark.sparkContext.parallelize(punc)
    # print (punc.collect())
    punc_list = spark.sparkContext.broadcast(punc)
    print (punc_list.value)

    def strip(x):
        for i, v in enumerate(x[1]):
            if len(v) > 1:
                if v[0] in punc_list.value:
                    x[1][i] = v[1:]
                if v[-1] in punc_list.value:
                    x[1][i] = v[:-1]
        return x


    def word_count(x):
        return [(v, 1) for i, v in enumerate(x[1])]

    def count_dict_per_doc(x):
        return (x[0][0],(x[0][1], x[1]))

    def reorder(x):
        '''
        reorder the indices of the documents
        '''
        pass

    def format_count(x):
        return [((v[0], x[1]), 1)for i, v in enumerate(x[0])]

    def string_count(x):
        return [((x[0], i), 1) for i in x[1]]


    # print (books.map(lambda x: (x[0], x[1].lower())).flatMap(lambda x: (x[0], x[1].split(' '))).collect())
    counts = books.map(lambda x: (x[0], x[1].lower())).flatMap(lambda x: [(x[0], x[1].split(' '))]).map(strip).map(word_count).zipWithIndex().map(format_count).\
    flatMap(lambda x: [(i[0], i[1]) for i in x]).reduceByKey(add).map(count_dict_per_doc).groupByKey().mapValues(list)
        #.zipWithIndex().map(get_count)#
    # #.groupByKey()
    # print (counts.type)
    # for x in counts:
    #     strip(x)
    #     print (x)
    # new_count = counts.filter(lambda x: x[0] not in swlist.value).collect()
    # print (counts.collect())
    # res = sorted(counts, key=lambda x:x[1], reverse = True) #sort the list as the result
    # res = res[0:int(sys.argv[1])] # get the top x number of words. x provided by sys.argv[1]
    res_file = os.path.join(script_dir, 's4.json')
    with open(res_file, 'w') as file:
        json.dump(counts.collect(), file)
