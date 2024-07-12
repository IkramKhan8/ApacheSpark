#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 12 03:37:00 2024

@author: ikramkhan
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,IntegerType, StringType
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

myschema = StructType([\
                       StructField("userID",IntegerType(), True),
                       StructField("name",StringType(), True),
                       StructField("age", IntegerType(), True),
                       StructField("friends",IntegerType(), True)                       
    ])
    
people = spark.read.format("csv")\
    .schema(myschema)\
        .option("inferSchema", "False")\
            .option("path","file:/Users/ikramkhan/ApacheSpark/fakefriends.csv")\
                .load()
                
people.printSchema()

output = people.select(people.userID,people.name\
                       ,people.age,people.friends)\
    .where(people.age < 30).withColumn('insert_ts', func.current_timestamp())\
        .orderBy(people.userID).cache()
        
output.createOrReplaceTempView("people")

spark.sql("select userID,name from people").show()

output.write\
    .format("csv")\
        .mode("overwrite")\
            .option("path","file:/Users/ikramkhan/ApacheSpark/SparkFiles/")\
                .partitionBy("age")\
                    .save()