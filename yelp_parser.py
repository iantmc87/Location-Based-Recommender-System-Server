'''
Author - Ian McManus
Date - 20/06/2019
Description - Parser for the yelp datasets
'''

#import functions
from pyspark.sql import SparkSession, SQLContext, functions as sf, Row
import time
from time import sleep
import pyspark
from pyspark.sql.functions import trim, regexp_replace, col, udf, struct, split, concat, lit, monotonically_increasing_id
from pyspark.sql.types import StringType, ArrayType, LongType, FloatType, DoubleType, StructType, IntegerType, StructField
import credentials as c

host = c.host
username = c.username
password = c.password
database = c.database
businessTable = c.businessTable
reviewTable = c.reviewTable
userTable = c.userTable
url = "jdbc:mysql://"+host+"/"+database+"?useSSL=False&useServerPrepStmts=false&rewriteBatchedStatements=true&user="+username+"&password="+password

'''
Method for removing none ascii characters
unicodeString = string to manipulate
'''
def nonasciitoascii(unicodeString):
	return unicodeString.encode("ascii", "ignore")

#creates the spark session for the recommendation system and adds the database connection drivers
spark = SparkSession.builder.appName("Yelp Parser").config("spark.driver.extraClassPath", "/usr/local/spark/jars/mysql-connector-java-5.1.47.jar").getOrCreate()

#creates the schema for the review dataset
reviewSchema = StructType([StructField('review_id',StringType(),nullable=True),StructField('user_id',StringType(),nullable=True),StructField('business_id',StringType(),nullable=True),StructField('stars',LongType(),nullable=True),StructField('date',StringType(),nullable=True),StructField('text',StringType(), nullable=True)])

uploadReviewSchema = StructType([StructField('review_id',StringType(),nullable=True),StructField('user_id',StringType(),nullable=True),StructField('business_id',StringType(),nullable=True),StructField('name',StringType(),nullable=True),StructField('stars',LongType(),nullable=True),StructField('date',StringType(),nullable=True),StructField('text',StringType(), nullable=True)])

#creates the schema for the yelp user dataset during initial creation
userSchema = StructType([StructField('user_id',StringType(),nullable=True),StructField('name',StringType(),nullable=True),StructField('review_count',LongType(),nullable=True),StructField('average_stars',DoubleType(),nullable=True)])

#creates the schema for the user table to be held in the SQL database
singleUserSchema = StructType([StructField('user_id', LongType(),nullable=True),StructField('user_name', StringType(),nullable=True),StructField('categories', StringType(),nullable=True),StructField('system', StringType(),nullable=True),StructField('latitude', DoubleType(),nullable=True),StructField('longitude', DoubleType(),nullable=True),StructField('radius', IntegerType(),nullable=True),StructField('alcohol', StringType(),nullable=True),StructField('good_for_kids', StringType(),nullable=True),StructField('good_for_groups', StringType(),nullable=True),StructField('dogs_allowed', StringType(),nullable=True),StructField('wheelchair_accessible', StringType(),nullable=True),StructField('price_range', StringType(),nullable=True),StructField('first_recommendation', LongType(),nullable=True),StructField('second_recommendation', LongType(),nullable=True),StructField('third_recommendation', LongType(),nullable=True),StructField('fourth_recommendation', LongType(),nullable=True),StructField('fifth_recommendation', LongType(),nullable=True),StructField('review_added', StringType(),nullable=True)])

#calls ascii converter method
convertedtoUDF = udf(nonasciitoascii)

#read in the three datasets
review = spark.read.json("/home/sparkuser/recommender_system/datasets/review.json", reviewSchema)
user = spark.read.json("/home/sparkuser/recommender_system/datasets/user.json", userSchema)
business = spark.read.json("/home/sparkuser/recommender_system/datasets/business.json").select("business_id","name","categories","review_count","stars","attributes.Alcohol","attributes.DogsAllowed","attributes.GoodForKids","attributes.RestaurantsGoodForGroups","attributes.RestaurantsPriceRange2","attributes.WheelchairAccessible")

#remove non ascii-characters
business = business.withColumn('nameConverted', convertedtoUDF(business.name))
business = business.drop('name')
business = business.withColumnRenamed('nameConverted', 'name')

#create auto incrementing id columns
schemaBusiness = business.withColumn('bid', lit(long(1)))
rdd_1 = business.rdd.zipWithIndex().map(lambda(row,rowId): (list(row)+[rowId+1]))
businessIncrementing = spark.createDataFrame(rdd_1, schema=schemaBusiness.schema)

schemaReview = review.withColumn('rid', lit(long(1)))
rdd_2 = review.rdd.zipWithIndex().map(lambda(row,rowId): (list(row)+[rowId+1]))
reviewIncrementing = spark.createDataFrame(rdd_2, schema=schemaReview.schema)

schemaUser = user.withColumn('uid', lit(long(1)))
rdd_3 = user.rdd.zipWithIndex().map(lambda(row,rowId): (list(row)+[rowId+1]))
userIncrementing = spark.createDataFrame(rdd_3, schema=schemaUser.schema)

#drops any businesses that arent restaurants
businessDrop  = businessIncrementing.filter("categories like '%Restaurant%'")

#updates review dataset with id numbers from business and user datasets
reviewDrop = reviewIncrementing.join(businessDrop, reviewIncrementing.business_id == businessDrop.business_id).select(reviewIncrementing.rid,reviewIncrementing.user_id,businessDrop.bid,reviewIncrementing.stars,reviewIncrementing.date,reviewIncrementing.text)
reviewDrop = reviewDrop.join(userIncrementing, reviewDrop.user_id == userIncrementing.user_id).select(reviewDrop.rid, userIncrementing.uid, reviewDrop.bid, userIncrementing.name, reviewDrop.stars, reviewDrop.date, reviewDrop.text)

#replaces true values with the name of the attribute
updateBusinessAttributes = businessDrop.withColumn('Alcohol', regexp_replace('Alcohol','none','Alcoholnone')).withColumn('Alcohol', regexp_replace('Alcohol','full_bar','Alcoholfullbar')).withColumn('Alcohol', regexp_replace('Alcohol','beer_and_wine','Alcoholbeerandwine')).withColumn('DogsAllowed', regexp_replace('DogsAllowed','True', 'DogsAllowed')).withColumn('GoodForKids', regexp_replace('GoodForKids','True','GoodForKids')).withColumn('RestaurantsGoodForGroups', regexp_replace('RestaurantsGoodForGroups','True','GoodForGroups')).withColumn('RestaurantsPriceRange2', regexp_replace('RestaurantsPriceRange2','1','PriceCheap')).withColumn('RestaurantsPriceRange2', regexp_replace('RestaurantsPriceRange2','2','PriceGoodvalue')).withColumn('RestaurantsPriceRange2', regexp_replace('RestaurantsPriceRange2','3','PriceAverage')).withColumn('RestaurantsPriceRange2', regexp_replace('RestaurantsPriceRange2','4','PriceExpensive')).withColumn('RestaurantsPriceRange2', regexp_replace('RestaurantsPriceRange2','2','Pricecheap')).withColumn('WheelchairAccessible', regexp_replace('WheelchairAccessible', 'True', 'WheelchairAccessible'))

#replaces all null values
removeNull = updateBusinessAttributes.na.fill('').replace('False', '')

#joins together the attributes columns into one
attributesJoin = removeNull.withColumn('attributes', sf.concat(sf.col('Alcohol'), lit(' '), sf.col('DogsAllowed'), lit(' '), sf.col('GoodForKids'), lit(' '), sf.col('RestaurantsGoodForGroups'), lit(' '), sf.col('RestaurantsPriceRange2'), lit(' '), sf.col('WheelchairAccessible'))).withColumn('postcode',lit(''))

#removes the no longer needed attributes columns
remove = attributesJoin.drop('business_id','Alcohol','DogsAllowed','GoodForKids','RestaurantsGoodForGroups','RestaurantsPriceRange2','WheelchairAccessible')

#renames id columns
renameBusiness = remove.withColumnRenamed('bid','business_id')
renameReviews = reviewDrop.withColumnRenamed('rid', 'review_id').withColumnRenamed('bid', 'business_id').withColumnRenamed('uid', 'user_id')

#reorders the business columns
businessUpload = renameBusiness.select('business_id', 'name', 'categories', 'review_count', 'stars', 'attributes', 'postcode')

#count business dataset
businessCount = businessUpload.count()
user_id = businessCount + 150000

#creates dummy data for user table for creation
rdd = spark.sparkContext.parallelize([{'user_id':user_id,
'user_name':"test_user",
'categories':"American, Italian, Indian, Mexican, Chinese, French",
'system':"Collaborative",
'latitude':0.0,
'longitude':0.0,
'radius':20,
'alcohol':"Alcoholnone, Alcoholfullbar, Alcoholbeerandwine",
'good_for_kids':"GoodForKids",
'good_for_groups':"GoodForGroups",
'dogs_allowed':"DogsAllowed",
'wheelchair_accessible':"WheelchairAccessible",
'price_range':"PriceCheap, PriceAverage, PriceGoodvalue, PriceExpensive",
'first_recommendation':businessUpload.collect()[businessCount - 1]['business_id'],
'second_recommendation':businessUpload.collect()[businessCount - 2]['business_id'],
'third_recommendation':businessUpload.collect()[businessCount - 3]['business_id'],
'fourth_recommendation':businessUpload.collect()[businessCount - 4]['business_id'],
'fifth_recommendation':businessUpload.collect()[businessCount - 5]['business_id'],
'review_added':"Yes"}]).toDF(schema = singleUserSchema)

#creates dummy review for the test user to use for the collaborative filtering system
testReviews = spark.sparkContext.parallelize([{'user_id':user_id, 'business_id':businessCount - 1, 'name':"test", 'stars':4, 'date':2019-01-12, 'text':"Test Review"}, {'user_id':user_id, 'business_id':businessCount - 2, 'name':"test", 'stars':5, 'date':2019-01-12, 'text':"Test Review"},{'user_id':user_id, 'business_id':businessCount - 3, 'name':"test", 'stars':4, 'date':2019-01-12, 'text':"Test Review"}, {'user_id':user_id, 'business_id':businessCount - 4, 'name':"test", 'stars':3, 'date':2019-01-12, 'text':"Test Review"}, {'user_id':user_id, 'business_id':businessCount - 5, 'name':"test", 'stars':2, 'date':2019-01-12, 'text':"Test Review"}, {'user_id':user_id, 'business_id':businessCount - 6, 'name':"test", 'stars':4, 'date':2019-01-12, 'text':"Test Review"}, {'user_id':user_id, 'business_id':businessCount - 7, 'name':"test", 'stars':5, 'date':2019-01-12, 'text':"Test Review"}, {'user_id':user_id, 'business_id':businessCount - 8, 'name':"test", 'stars':4, 'date':2019-01-12, 'text':"Test Review"}, {'user_id':user_id, 'business_id':businessCount - 9, 'name':"test", 'stars':3, 'date':2019-01-12, 'text':"Test Review"}, {'user_id':user_id, 'business_id':businessCount - 10, 'name':"test", 'stars':5, 'date':2019-01-12, 'text':"Test Review"}] ).toDF(schema = uploadReviewSchema)

reviewJoined = testReviews.union(renameReviews)

#writes datasets to mySQL database
rdd.write.option("truncate","true").jdbc(url, table=userTable,mode="errorIfExists")
businessUpload.write.option("truncate","true").jdbc(url, table=businessTable,mode="errorIfExists")
reviewJoined.write.option("truncate","true").jdbc(url, table=reviewTable,mode="errorIfExists")

