# PageRank project

## Overview
In this project, I implemented a movie recommendation system using item CF algorithm on Hadoop MapReduce based on users' history data.

## Main Steps

* Preprocessed the source data to get userRating.txt.

* Built co-occurrence matrix.

* Built rating matrix.

* Implemented matrix computation.

* Optimized the system to overcome lack of rating history data.

## Deploy
Deployed a Hadoop cluster on Docker, which has one master node and two slave nodes.

Data preprocessing

* Changed the raw data into the following format: userId, movieId, rating.


There are total six MapReduce job:

* MatrixGeneratorMapper: used userRating.txt to build relationship between two movies: <movieA, movieB>

* MatrixGeneratorReducer: summed up user rating lists

* NormalizeMapper: read output generated from MatrixGeneratorReducer

* NormalizeReducer: performed normalization to get co-occurrence matrix cell

* AverageRatingMapper: used userRating.txt to calculate each user rating list: userId : movieId = rating

* AverageRatingReducer: calculated average rating for each userId

* ReadMapper: read output generated from AverageRatingReducer

* RatingMapper: used userRating.txt to calculate each user rating list: userId : movieId = rating

* preProcessRatingReducer: used average rating to fill in non-rated movie for each userId to get rating matrix

* CooccurrenceMapper: read in output from NormalizeReducer

* RatingMapper: read in output from preProcessRatingReducer

* MultiplicationReducer: performed matrix multiplication of co-occurrence matrix and rating matrix.

* SumMapper: read in output from MultiplicationReducer

* SumReducer: summed up results from mappers and generate PageRank results

```
$ hadoop com.sun.tools.javac.Main *.java
$ jar cf recommender.jar *.class
$ hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrix /Normalize /Multiplication /Sum
```


* args0: original dataset
* args1: output directory for DividerByUser job
* args2: output directory for coOccurrenceMatrixBuilder job
* args3: output directory for Normalize job
* args4: output directory for Multiplication job
* args5: output directory for Sum job
