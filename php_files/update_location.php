<?php
	
/*
* Author - Ian McManus
* Date - 20/06/2019
* Version - 1.0.0
* Description - Updates database with current location
*/

	#imports database connection file and variables
	require 'db_connect.php';
	global $connect, $userTable;

	#gets data from the application
	$longitude = $_POST["longitude"];
	$latitude = $_POST["latitude"];
	$user = $_POST['user_name'];

	#sends location to the database
	$query = "UPDATE $userTable SET longitude = $longitude, latitude = $latitude WHERE user_name = '$user'";
	mysqli_query($connect, $query) or die (mysqli_error($connect));
	mysqli_close($connect);

?>
