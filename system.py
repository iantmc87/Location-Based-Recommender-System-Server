#import functions
from pyspark.sql import SparkSession, SQLContext, functions as sf, Row
import time
from time import sleep
import pyspark
from pyspark.sql.functions import regexp_replace, col, udf, struct, split
from pyspark.sql.types import StringType, ArrayType, FloatType, DoubleType, StructType, IntegerType, StructField
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.ml.feature import HashingTF, IDF, Normalizer#, Tokenizer, StopWordsRemover, CountVectorizer
import credentials as c

#creates the spark session for the recommendation system and adds the database connection drivers
spark = SparkSession.builder.appName("Recommendation System").config("spark.driver.extraClassPath", "/usr/local/spark/jars/mysql-connector-java-5.1.47.jar").getOrCreate()

#creates variables
host = c.host
username = c.username
password = c.password
database = c.database
reviewTable = "(SELECT review_id, user_id, business_id, stars FROM " + c.reviewTable + ") AS tmp"
userTable = 'user'
geolocationTable = "(SELECT geolocation_id, postcode, latitude, longitude FROM " + c.geolocationTable + ") AS tmp"
businessTable = "(SELECT business_id, categories, attributes, postcode FROM " + c.businessTable + ") AS tmp"
recommendations = None
model = None
url = "jdbc:mysql://"+host+"/"+database+"?useSSL=False&user="+username+"&password="+password
start_time = None
idleIntervalInSeconds = 2 

'''
method for creating and training the ALS model
returns the model to be used for the collaborative filtering system
'''
def alsModel ():
	
	places = spark.sql("SELECT user_id, business_id, stars FROM review").cache()
	train, test = places.randomSplit([0.7,0.3])
	train.cache()
	test.cache()
	rank = 5
	numIterations = 10
	model = ALS.train(train, rank, numIterations)

	return model
#end ALS method

'''
method for importing SQL tables into memory
table = SQL table name
partition column = wiich column to use for partitioning
upperBound = number of rows in dataset
numPartitions = how many partitions to use when splitting the dataset
tempTable = name of table to be used in memory
'''
def importSQL (table, partitionColumn, upperBound, numPartitions, tempTable ):
	spark.read.format("jdbc").option("url", url).option("driver", "com.mysql.jdbc.Driver").option("dbtable", table).option("partitionColumn", partitionColumn).option("lowerBound", "0").option("upperBound", upperBound).option("numPartitions", numPartitions).load().createOrReplaceTempView(tempTable)
	spark.table(tempTable).cache()
#end import sql method

'''
method for concatenation of the attributes columns into one one column
table = name of user table holding the attributes
returns the final table with all the attributes joined
'''
def concatAttributes(table):

	userAttributes = spark.sql("SELECT user_id AS business_id, categories, CONCAT(alcohol,\' \',good_for_kids,\' \',good_for_groups,\' \',dogs_allowed,\' \',wheelchair_accessible,\' \',price_range) AS attributes FROM {0}".format(table))

	return userAttributes
#end concat attributes method


'''
method for cleaning the dataframe to be used with the content-based system
table = name of dataframe to be cleaned
returns dataframe with commas and [ ] removed
'''
def dataClean(table):

	removeCommas = table.withColumn('categories', regexp_replace(col('categories'), ",",""))
	concat = removeCommas.withColumn('key_words', sf.concat(sf.col('categories'), sf.lit(' '), sf.col('attributes')))
	drop = concat.drop('categories', 'attributes')
	removeBrackets = drop.withColumn('key_words', regexp_replace(col('key_words'), "\[",""))
	removeBrackets = removeBrackets.withColumn('key_words', regexp_replace(col('key_words'), "\]",""))
	removeBrackets = removeBrackets.withColumn('key_words', split('key_words', ' '))

	return removeBrackets
#end data clean method

'''
method for getting the term frequency of words and normalising them to be checked for similarity
table = name of dataframe to be used
returns the dataframe with the data normalised
'''
def termFrequency(table):

	#calculates the term frequency of attributes
	hashingTF = HashingTF(inputCol='key_words', outputCol='hashing')
	tf = hashingTF.transform(table)
	tf.cache()

	#normalises the term frequency data
	normalizer = Normalizer(inputCol='hashing', outputCol='norm')
	term = normalizer.transform(tf)

	return term
#end term frequency method

'''
method for generating the cosine similarity between the user and businesses
table = name of dataframe to be used
returns the top 5 businesses that are similar to the users chosen preferences
'''
def cosineSimilarity(table):

	dot_udf = sf.udf(lambda x, y: float(x.dot(y)), DoubleType())
	sort = table.alias('user').join(table.alias('product'), sf.col('user.business_id') < sf.col('product.business_id')).select(sf.col('user.business_id').alias('user'), sf.col('product.business_id').alias('product'), dot_udf('user.norm', 'product.norm').alias('similarity')).sort('user', 'product')
	sort.createOrReplaceTempView('sorting')

	recommendations = spark.sql("SELECT * FROM sorting WHERE product = {0} ORDER BY similarity DESC LIMIT 5".format(user_id))

	return recommendations
#end cosine similarity method

#imports SQL tables into memory
importSQL(businessTable, "business_id", "200000", "10", "business")
importSQL(geolocationTable, "geolocation_id", "2100000", "10", "geolocation")
importSQL(reviewTable, "review_id", "6000000", "10", "review")

#runs the training and testing for the collaborative filtering approach on the reviews datasett in memory
model = alsModel()

#while loop for running the recommender systems
while True:

	start_time = time.time()
	
	#reads the user database into memory from SQL
	importSQL(userTable, "user_id", 100, 1, "user")
	users = spark.sql("SELECT * FROM user WHERE LENGTH(latitude) > 4")

	#if statement for when a location has been updated in the database
	if users.count() > 0:
		
		#creates the table in memory for user dataset
		users.createOrReplaceTempView("users")
		spark.table('users').cache()
		
		#creates the variables from the user dataset
		for key, value in spark.table("users").first().asDict().items():
			globals()[key] = value

		#if statement for if the user chooses collaborative
		if system == "Collaborative":

			#if statement for when a review is added
			if review_added == 'Yes':
				
				importSQL(reviewTable, "review_id", "6000000", "10", "review")			
				#re-runs training model if review been added
				model = alsModel()
			
			#recommends the restaurants for the user		
			userModel = model.recommendProducts(user_id, 100)
			
			#creates the dataframe with the user recommendations
			spark.createDataFrame(userModel).createOrReplaceTempView("userRecommendations")
		
			#selects the recommended restaurants within the users specified locality
			recommendations = spark.sql("SELECT user, product, rating FROM userRecommendations WHERE product IN (SELECT business_id FROM business b INNER JOIN geolocation g ON b.postcode = g.postcode WHERE (acos(sin(g.latitude * 0.0175) * sin({0} * 0.0175) + cos(g.latitude * 0.0175) * cos({0} * 0.0175) * cos(({1} * 0.0175) - (g.longitude * 0.0175))) * 3959 <= {2})) ORDER BY rating DESC".format(latitude, longitude, radius))

		#if statement for if the user chooses content-based
		elif system == "Content-Based":

			#selects businesses within the specified locaility of the user
			withinRadius1 = spark.sql("SELECT business_id, categories, attributes FROM business b INNER JOIN geolocation g ON b.postcode = g.postcode WHERE (acos(sin(g.latitude * 0.0175) * sin({0} * 0.0175) + cos(g.latitude * 0.0175) * cos({0} * 0.0175) * cos(({1} * 0.0175) - (g.longitude * 0.0175))) * 3959 <= {2}) LIMIT 2000".format(latitude, longitude, radius))			
			
			#joins the user and business dataframes together
			withinRadius = concatAttributes('users').union(withinRadius1)
			
			#runs the clean data method
			cleanData = dataClean(withinRadius)
			
			#runs the term frequency method
			terms = termFrequency(cleanData)
			
			#runs the cosine similarity method
			recommendations = cosineSimilarity(terms)
						
		#creates the dataframe to upload to the database
		rdd = spark.sparkContext.parallelize([{'user_id':user_id,
		'user_name':user_name,
		'categories':categories,
		'system':system,
		'latitude':53.7544,
		'longitude':-2.366,
		'radius':radius,
		'alcohol':alcohol,
		'good_for_kids':good_for_kids,
		'good_for_groups':good_for_groups,
		'dogs_allowed':dogs_allowed,
		'wheelchair_accessible':wheelchair_accessible,
		'price_range':price_range,
		'first_recommendation':recommendations.collect()[0]['user'],
		'second_recommendation':recommendations.collect()[1]['user'],
		'third_recommendation':recommendations.collect()[2]['user'],
		'fourth_recommendation':recommendations.collect()[3]['user'],
		'fifth_recommendation':recommendations.collect()[4]['user'],
		'review_added':'No'}]).toDF(schema = spark.table('user').schema)		
		otherUsers = spark.sql("SELECT * FROM user WHERE LENGTH(latitude) < 4")

		#recreates the user dataset with the new data
		allUsers = otherUsers.union(rdd)
		
		#writes the database to SQL
		allUsers.write.option("truncate", "true").jdbc(url, table=userTable,mode="overwrite")

	print "My Program took", time.time() - start_time, "to run"
	
	#put the system to sleep for specified number of seconds
	sleep(idleIntervalInSeconds)


