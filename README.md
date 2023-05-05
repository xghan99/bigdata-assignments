# bigdata-assignments
This repository consists of code I wrote for assignments from CS4225 - Big Data Systems for Data Science.

## Common Words
This assignment requires us to write a MapReduce function to output the top k words that are common and have length greater than 4, given 2 textual files. The frequency is computed by the smaller number of times it appears between the 2 files. The output is then sorted by frequency in descending order, and ties are broken in ascending lexicographic order. Commonly used stopwords are removed from consideration. The code is written in Java and utilises the Apache Hadoop MapReduce framework

## Spark SQL and Spark ML
The first part of the assignment requires us to use SparkSQL to compute aggregated results given 3 relational tables. The second part of the assignment requires us to use Spark MLlib to build a pipeline to perform data pre-processing and train a Machine Learning model on a bank default dataset. The code was run on the Databricks platform.
