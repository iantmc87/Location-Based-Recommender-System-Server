# Location-Based-Recommender-System-Server
Location based recommender system for restaurants - Server Side

The project recommends restaurants to users based on their location and filtering preferences. It is created using python and held upon an Apache Spark cluster 
to increase execution times by running the computations in memory. Two recommendation filtering approaches are used which are collaborative (based on other users' 
ratings of the restaurants) and content-based (based on the characteristics of a restaurant).

The front-end for the recommender system is a mobile application which can be found at ****

<hr>

<h3>Prerequisites</h3>
<ul><li>One or more connected linux servers (original setup 6 * 32GB servers - 1 Master & Slaves)</li>
<li>LAMP stack installed on each server with Ubuntu 16.04 Server Edition (MySQL & PHP)</li>
<li>Each server with the same credentials - Username = sparkuser & Password = password (Can be different but log-in details need changing in credentials.py and db_connect.php files)</li>
<li>Datbase created on the master server called recommender_system</li>
<li>Latest version of Apache Spark downloaded and installed following the guide in apache_spark.txt</li>
<li>Business, Review and User datasets downloaded and extracted from yelp.com dataset challenge</li></ul>

<hr>


<h3>How to Run</h3>
<ul><li>Clone or download the this repository and place in a folder on the master server called recommender_system</li>
<li>Move the files within php_files to /var/www/html folder</li>
<li>Move the jar file to /usr/local/spark/jars folder</li>
<li>Rename the yelp datasets as business.json, review.json and user.json and place within the datasets folder</li>
<li>Extract the csv file from geolocation.tar.gz file using following command</li></ul>

```
sudo tar -zxvf geolocation.tar.gz
```

<ul><li>open geolocation.py and enter postcode of the location to update the business locations to</li>
<li>Navigate to /usr/local/spark/sbin and enter</li></ul>

```
./start-all.sh
```

<ul><li>Navigate to /usr/local/spark/bin and enter the following to parse the yelp datasets into a readable format</li></ul>

```
./spark-submit --deploy-mode client --master spark://localhost:7077 /home/sparkuser/recommender_system/yelp_parser.py
```

<ul><li>Then enter the following to update the businesses with required locations </li></ul>

```
./spark-submit --deploy-mode client --master spark://localhost:7077 /home/sparkuser/recommender_system/geolocation.py</li>
```

<ul><li>Access the SQL database and updating the user table setting user_id as Primary Key and user_name as Unique Key</li>
<li>Enter the following in /usr/local/spark/bin to run the recommender system </li></ul> 

```
./spark-submit --deploy-mode client --master spark://localhost:7077 /home/sparkuser/recommender_system/system.py</li>
```

<ul><li>Then either update the fields in the user table using SQL queries to test the system or install the mobile application front end on your device using the link at the top of the readme file</li></ul>

<hr>

<h3>Future Developments</h3>
<ul><li>Add the ability for multiple users to access recommendations concurrently</li>
<li>Create the system in Scala instead of Python to see if any performance difference</li>
<li>Try different recommender system filtering algorithms inlcuding adding a hybrid approach</li>
<li>Add sentiment analysis using the textual reviews in the dataset, instead of just using the ratings given by users</li>
<li>Source real-world UK restaurant data instead of using american restaurants combined with UK locations</li></ul>
