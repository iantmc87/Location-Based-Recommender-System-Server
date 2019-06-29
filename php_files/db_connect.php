<?php

/*
* Author - Ian McManus
* Date - 20/06/2019
* Version - 1.0.0
* Description - connection to the SQL database
*/

	#database credentials	
	$server = "localhost";
	$database = "recommender_system";
	$username = "sparkuser";
	$password = "password";

	#database table names
	$businessTable = "business";
	$userTable = "user";
	$reviewsTable = "review";
	$geolocationTable = "geolocation";

	#try/catch statement for creating connection to database
	try {
		$connect = mysqli_connect($server, $username, $password, $database);
	} catch (Exception $e) {
		echo "Database Connection Error";	
	}

?>
