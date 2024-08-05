# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df=spark.read.csv("/FileStore/gmde/googleplaystore__1_.csv")

# COMMAND ----------

st=StructType().add("App",StringType(),True)\
            .add("Category",StringType(),True)\
            .add("Rating",DoubleType(),True)\
            .add("Reviews",IntegerType(),True)\
            .add("Size",StringType(),True)\
            .add("Installs",StringType(),True)\
            .add("Type",StringType(),True)\
            .add("Price",StringType(),True)\
            .add("Content Rating",StringType(),True)\
            .add("Genres",StringType(),True)\
            .add("Last Updated",StringType(),True)\
            .add("Current Ver",StringType(),True)\
            .add("Android Ver",StringType(),True)

# COMMAND ----------

df=spark.read.option("header",True).schema(st).csv("/FileStore/gmde/googleplaystore__1_.csv")

# COMMAND ----------

df.select("Type","Price").show()

# COMMAND ----------

data = [
    ("Alice", ["apple", "banana", "cherry"]),
    ("Bob", ["orange", "melon"]),
    ("Cathy", ["grape", "kiwi", "lemon"])
]

df_explod = spark.createDataFrame(data, ["name", "fruits"])
df_explod.show()

# COMMAND ----------

explosion = df_explod.withColumn("fruit", explode(df_explod.fruits))
explosion.show()

# COMMAND ----------

df.select(col('Reviews'),col('Rating')+1).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df_when=df.select("Rating",when(col("Rating")>3,1).otherwise(0).alias("answer"))
df_when.display()

# COMMAND ----------

df_like=df.select("Rating",col("Content Rating").like("Teen"))
df_like.display()

# COMMAND ----------

df_substr=df.select(df.Genres.substr(1, 5).alias("new_gen"))
df_substr.display()

# COMMAND ----------

df.select(col('Rating').between(4, 5)).display() 

# COMMAND ----------

df_gb=df.groupBy("App").count()
df_gb.display()

# COMMAND ----------

df_filter=df.filter(col("Rating")>4)
df_filter.display()
