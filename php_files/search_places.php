<?php

/*
* Author - Ian McManus
* Date - 20/06/2019
* Version - 1.0.0
* Description - updates database with chosen location for searching
*/
	#imports database connection file and variables
	require 'db_connect.php';
	global $connect, $userTable, $geolocationTable;
	
	#gets data from the application
	$search = $_POST['search'];
	$user = $_POST['user_name'];

	#sends data to the database
	$query = "UPDATE $userTable u SET u.latitude = (SELECT g.latitude FROM $geolocationTable g WHERE g.postcode = '$search' LIMIT 1), u.longitude = (SELECT ge.longitude FROM $geolocationTable ge WHERE ge.postcode = '$search' LIMIT 1) WHERE u.user_name = '$user'";
	mysqli_query($connect, $query) or die (mysqli_error($connect));
	mysqli_close($connect);

?>
