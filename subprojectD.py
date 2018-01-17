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
from __future__ import division
import json, sys, os
from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext
from collections import OrderedDict
import math

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
        .appName("tfidf")\
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
    print (punc)

    punc_list = spark.sparkContext.broadcast(punc)
    print (punc_list.value)

    def strip(x):
        '''
        Strip words whose first or last character is a punctuation. (But the project may not need this complicated process?)
        '''
        for i, v in enumerate(x[1]):
            if len(v) > 1:
                if v[0] in punc_list.value:
                    x[1][i] = v[1:]
                if v[-1] in punc_list.value:
                    x[1][i] = v[:-1]
        return x


    def word_count(x):
        '''
        Count words in each document.
        '''
        return [(v, 1) for i, v in enumerate(x[1])]

    def count_dict_per_doc(x):
        '''
        Transform ((word, docname), count) into (word, (count, doc)). The purpose is for tfidf calculations.
        '''
        return (x[0][0],(x[0][1], x[1]))

    def format_count(x):
        '''
        for each word in each document, we return ((word, docname), 1)
        '''
        return [((v[0], x[1]), 1) for i, v in enumerate(x[0])]

    def string_count(x):
        return [((x[0], i), 1) for i in x[1]]

    def tfidf(x):
        '''
        Get the tfidf score for each word.
        '''
        return [x[0], [(v_sub[0], v_sub[1] * math.log(maxn/len(x[1]))) for i_sub, v_sub in enumerate(x[1])]]

    #count words in RDD
    counts = books.map(lambda x: (x[0], x[1].lower())).flatMap(lambda x: [(x[0], (' '.join(x[1].splitlines())).split())]).map(strip).map(word_count).zipWithIndex().map(format_count).\
    flatMap(lambda x: [(i[0], i[1]) for i in x]).reduceByKey(add).map(count_dict_per_doc).groupByKey().mapValues(list)

    maxn = max([len(v_main[1]) for i_main, v_main in enumerate(counts.collect())])   #find the maximum length of v_main[1], which is the total number of documents

    counts = counts.map(tfidf)  #apply tfidf function
    l = counts.collect()        #collect counts data from RDD into Python
    k = [[i[0], [0]*maxn] for i in l]   #build another dummy list for maniputation (and eventually ranking the tfidf scores)
    for i_main, v_main in enumerate(l):    #fill in the values for dummy list k
        for i_sub, v_sub in enumerate(v_main[1]):
            k[i_main][1][v_sub[0]] = v_sub[1]
    k = [sorted(k, key = lambda x:x[1][n] ,reverse=True)[0:5] for n in range(maxn)]   #sort list k and select words with top 5 tfidf scores  in each document
    ans = [(k[0],k[1][i]) for i, v in enumerate(k) for k in v]    #transform the results of list k into an answer list

    #write in a json file as the output
    res_file = os.path.join(script_dir, 'sp4.json')
    with open(res_file, 'w') as file:
        json.dump(dict(ans), file)
