#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.ml.feature import Bucketizer
import pyspark.sql.functions as f
from pyspark.sql.functions import isnan, when, count, col


# In[5]:


class eda_pyspark_perosnal_col:
    
    def __init__(self,filename):
        self.df = self.reader_func(filename)


    def reader_func(self,filename):
        type_file = filename.split('.')[1]
        if type_file=='csv':
            return spark.read.csv(filename)
        if type_file=='json':
            return spark.read.json(filename)

    def value_count_matrix(self,col1,col2):
        df = self.df
        return df.crosstab(col1,col2)

    def create_hist(self,colname):
        df = self.df
        df.groupBy(colname).count().rdd.values().histogram()

    def null_missing_calculator(self):
        df = self.df
        df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

