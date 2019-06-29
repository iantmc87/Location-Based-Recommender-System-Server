'''
Author - Ian McManus
Date - 20/06/2019
Description - Uploading location data to SQL Database
'''

import credentials as c
import pyspark
import pandas
from pyspark.sql import SparkSession, SQLContext, functions as sf, Row
from pyspark.sql.functions import struct, monotonically_increasing_id
from pyspark.sql.types import StringType, DoubleType, StructType, StructField

#enter desired postcode for business locations to be updated to
POSTCODE = "*** ****"
host = c.host
username = c.username
password = c.password
database = c.database
businessTable = c.businessTable
geolocationTable = c.geolocationTable

#database connection string
url = "jdbc:mysql://"+host+"/"+database+"?useSSL=False&useServerPrepStmts=false&rewriteBatchedStatements=true&user="+username+"&password="+password

#creates the spark job
spark = SparkSession.builder.appName("Geolocation Parser").config("spark.driver.extraClassPath", "/usr/local/spark/jars/mysql-connector-java-5.1.47.jar").getOrCreate()

#creates the geolocation schema
locationSchema = StructType([StructField('postcode',StringType(),nullable=True),StructField('latitude',DoubleType(),nullable=True),StructField('longitude',DoubleType(),nullable=True),StructField('district',StringType(),nullable=True),StructField('city',StringType(),nullable=True),StructField('county',StringType(),nullable=True)])

#reads in geolocation csv file into memory
location = spark.read.csv('/home/sparkuser/recommender_system/geolocation.csv', locationSchema).createOrReplaceTempView('location');
spark.table('location').cache()

#reads in business dataset from SQL into memory
businessSQL = spark.read.format('jdbc').option('url', url).option('driver', 'com.mysql.jdbc.Driver').option('dbtable', businessTable).load().createOrReplaceTempView('business')
spark.table('business').cache()

#gets latitude and longitude for users postcode to get postcodes close to location
userSelection = spark.sql("SELECT latitude, longitude FROM location WHERE postcode = '{0}'".format(POSTCODE)).createOrReplaceTempView('selection')
latitude = spark.table('selection').collect()[0]['latitude']
longitude = spark.table('selection').collect()[0]['longitude']

#selects postcodes from within radius of chosen postcode
radius = spark.sql("SELECT postcode as postcode1, latitude, longitude, district, city, county FROM location g WHERE (acos(sin(g.latitude * 0.0175) * sin({0} * 0.0175) + cos(g.latitude * 0.0175) * cos({0} * 0.0175) * cos(({1} * 0.0175) - (g.longitude * 0.0175))) * 3959 <= 16)".format(latitude, longitude)).createOrReplaceTempView('radius')

#creates common column for joining datasets
temp = spark.table('business').withColumn('row_id', monotonically_increasing_id())
temp2 = spark.table('radius').withColumn('row_id', monotonically_increasing_id())

#joins the datasets
businessTemp = temp.join(temp2, 'row_id').drop('row_id').createOrReplaceTempView('businessTemp')

#selects the business fields to be uploaded to database
businessUpload = spark.sql("SELECT business_id, name, categories, review_count, stars, attributes, postcode1 as postcode FROM businessTemp")

#selects the geolocation fields to be uploaded to database
locationUpload = spark.sql("SELECT postcode1 as postcode, latitude, longitude, district, city, county FROM radius")

#writes datasets to database
locationUpload.write.jdbc(url, table=geolocationTable, mode="errorIfExists")
businessUpload.write.option("truncate","true").jdbc(url, table=businessTable, mode="overwrite")


