# PageRank project

## Overview
In this project, I implemented PageRank algorithm and deployed using Hadoop MapReduce in Java.

## Main Steps

* Preprocessed the source data to get transition.txt and PR.txt files.

* Built transition matrix from file transition.txt to represent relationship model between webpages.

* Built PageRank matrix from PR.txt to calculate the weight between webpages.

* Using Teleporting to handle edge cases like spider traps and dead ends.

* Using Python http server to show the result.

## Web Interface

Demo looks like


![](demo.gif)

## Deploy
Deployed a Hadoop cluster on Docker, which has one master node and two slave nodes.

Raw data is from this [website](https://www.limfinity.com/ir/).

Data preprocessing

* Mapped a unique tag to each website.

* Changed the raw data into the following format: fromPageId toPageId.


There are two MapReduce job here, both of them has two mappers and one reducer.

* TransitionMapper: used transition.txt to generate transition matrix cell

* PRMapper: used PR.txt to generate PageRank matrix cell

* MultiplicationReducer: calculated results of matrix cell * PageRank cell

* PassMapper: read output generated from MultiplicationReducer

* BetaMapper: implemented Teleporting to enhance the model

* SumReducer: summed up results from mappers and generate PageRank results

```
$ hadoop com.sun.tools.javac.Main *.java
$ jar cf ngram.jar *.class
$ hadoop jar pr.jar Driver /transition /pagerank /output 5 0.2
```

* args0: dir of transition.txt
* args1: dir of PR.txt
* args2: dir of unitMultiplication result
* args3: times of convergence
* args4: value of beta
