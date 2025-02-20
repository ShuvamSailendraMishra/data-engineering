# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, DoubleType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "b95fe8e1-a0b4-48f1-b4ca-e8d2d0abd5c0",
"fs.azure.account.oauth2.client.secret": 'gL68Q~wttkQW1BVt_D_dd~4Xl9QIVXGySGxUJca8',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d857f163-196c-46fd-b149-b92c3ac4037d/oauth2/token"}

dbutils.fs.mount(
source = "abfss://olympic-data@olympicdatashuvam.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/olympicdata",
extra_configs = configs)





# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/olympicdata"

# COMMAND ----------

coaches = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/olympicdata/raw-data/data/coaches.csv")
EntriesGender = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/olympicdata/raw-data/data/EntriesGender.csv")
medals = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/olympicdata/raw-data/data/medals.csv")
teams = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/olympicdata/raw-data/data/teams.csv")

# COMMAND ----------

coaches.show()

# COMMAND ----------

EntriesGender.show()

# COMMAND ----------

EntriesGender.printSchema()

# COMMAND ----------

EntriesGender = EntriesGender.withColumn("Female",col("Female").cast(StringType()))\
    .withColumn("Male",col("Male").cast(StringType()))\
    .withColumn("Total",col("Total").cast(StringType()))

# COMMAND ----------

EntriesGender.printSchema()

# COMMAND ----------

EntriesGender = EntriesGender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))

# COMMAND ----------

EntriesGender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

topmedals = medals.orderby("Gold", "Silver", "Bronze", "Total", ascending=Ture).select("TeamCountry", "Gold").show()

# COMMAND ----------

topmedals = medals.orderby("Gold", "Silver", "Bronze", "Total", ascending=True).select("TeamCountry", "Gold").show()

# COMMAND ----------

topmedals = medals.orderBy("Gold", "Silver", "Bronze", "Total", "Rank by Total", ascending=False).select("TeamCountry", "Rank by Total").show()

# COMMAND ----------

avg_entries = EntriesGender.withColumn("AvgFemale", EntriesGender["Female"] / EntriesGender["Total"])\
    .withColumn("AvgMale", EntriesGender["Male"] / EntriesGender["Total"])

# COMMAND ----------

avg_entries.show()

# COMMAND ----------

coaches.write.mode("overwrite").option("header", "true").csv("mnt/olympicdata/transformed-data/coaches")

# COMMAND ----------

avg_entries.write.mode("overwrite").option("header", "true").csv("mnt/olympicdata/transformed-data/entriesgender")