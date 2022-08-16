from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import openpyxl

spark = SparkSession \
    .builder \
    .appName("q1 Tutorial") \
    .getOrCreate()

# DATA EXTRACTING
data = "/Users/gigi/Desktop/thesis/data_updated.csv"

# Reading csv file with Spark
sparkData = spark.read.csv(data, inferSchema=False, header=True, sep=",")

# Defining data schema
schema = StructType([StructField("Row ID", IntegerType(), False),
                      StructField("Order ID", StringType(), False),
                      StructField("Order Date", StringType(), False),
                      StructField("Ship Mode", StringType(), False),
                      StructField("Customer ID", StringType(), False),
                      StructField("Postal Code", StringType(), False),
                      StructField("Product ID", StringType(), False),
                      StructField("Category", StringType(), False),
                      StructField("Sub-Category", StringType(), False),
                      StructField("Sales", DoubleType(), False)])

# Reading data with defined schema
sparkData = spark.read.csv(data, schema=schema, header=True, sep=",")

# DATA TRANSFORMING

# Formatting Date column
sparkDAta = sparkData.withColumn("Date", to_date(col("Order Date"), "dd-MM-yyyy"))

# Rounding Sales column to 2 digits
sparkData = sparkData.withColumn("Sales", round(sparkData["Sales"], 2))


# USD to EURO Conversion and rounding to 2 digits
sparkData = sparkData.withColumn("Sales(€)", sparkData.Sales * 0.95)
sparkData = sparkData.withColumn("Sales(€)", round(sparkData["Sales(€)"], 2))
df0 = sparkData.select("Order ID", "Sales", "Sales(€)")

#
df1 = sparkData.select("*").where((sparkData['Postal Code'] == "42420") | (sparkData['Postal Code'] == "84084")).\
          groupby('Postal Code').sum('Sales(€)')

df2 = sparkData.select("Customer ID", "Sales(€)").where(sparkData['Ship Mode'] == "1st Class").\
    where((sparkData['Sub-Category'] == "Binders") | (sparkData['Sub-Category'] == "Paper") | (
            sparkData['Sub-Category'] == "Labels"))

df3 = sparkData.select("Ship Mode", "Category", "Sales(€)").where(sparkData["Customer ID"] == "BH-11710").groupby(
    "Category", "Ship Mode").avg("Sales(€)")


with pd.ExcelWriter('outputs.xlsx', engine='openpyxl') as writer:
    df0.toPandas().to_excel(writer, sheet_name='query1', index=False)
    df1.toPandas().to_excel(writer, sheet_name='query2', index=False)
    df2.toPandas().to_excel(writer, sheet_name='query3', index=False)
    df3.toPandas().to_excel(writer, sheet_name='query4', index=False)



