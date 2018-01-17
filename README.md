# whusym-p0:  basic NLP in a distributed environment
This is Yuanming (Jeremy) Shi's project 0 for Data Science Practicum (Sp 18) at the University of Georgia. This project is about basic word counts in a documents and calculate TF-IDF scores across different documents. Project materials and instructions can be found [here](https://github.com/dsp-uga/sp18/tree/master/projects/p0).  Here are what different Python files in my repo are about:

* *subprojectA.py*: preprocessing (removing newline markers and empty str) and return top 40 words across all the documents in the /data folder.
* *subprojectB.py*: remove stop words based on the results of subprojectA.
* *subprojectC.py*: remove punctuation markers (if they appear as the first or last character of a keyword) based on the results of subprojectB.
* *subprojectD.py*: this is the most challenging one. This subproject is to compute TF-IDF scores for each word in each document. TF stands for term frequency (in each document). And IDF stands for inverse document frequency. This file returns top 5 words (ranked by on their TF-IDF scores) in each document.

# Dependencies
All the scripts are written in Python (and should be compatible with both Python 2.7 and 3.5+. Although compatibility for 2.7 still needs to be further tested). Moreover, all the scripts are based on [Apache Spark (2.2.1)](https://spark.apache.org/).

# How to run
1. Make sure you have all the dependencies installed.
2. Clone or download the repo and unzip it.
3. In order to set up master-slave(driver-worker) environment in Spark, you first need to go to your spark folder and run the following (depending on your setting of Spark, the number of workers will vary):
```
./sbin/start-all.sh
```

4. To start each subproject, you need to go to your apache spark folder, and run the following command:
```
./bin/spark-submit --master [YOUR-SPARK-MASTER-ADDRESS] [**kwargs]
```
For instance,
```
./bin/spark-submit --master spark://1.1.1.1:7077 /Users/foo/whusym-p0/subprojectC.py 40 stopwords.txt punc.txt
```

is a command to run *subprojectC.py* at the directory */Users/foo/whusym-p0/subprojectC.py* with first *40* top words, and the stopwords file is *stopwords.txt*, and the punctuation list is in *punc.txt* (Here both txt files need to be at the same directory with the aforementioned python script). Different subprojects need different numbers of kwargs. In addition to the file path, subprojectA needs 1, subprojectB needs 2, and subprojectC and subprojectD need 3, respectively.

If you have workers set on the master, the scripts should run on workers as well. If you cannot find your spark master IP address or you want to see your the status of workers, you can check on http://localhost:8080/.

If you run into any problems, please create a ticket!


# TODO
* Running more tests on subprojectD to improve TF-IDF performances.

# Acknowledgements
Great thanks to [Dr. Shannon Quinn](cobweb.cs.uga.edu/~squinn/) for his instructions. I also benefited from my discussions with Jenny.
