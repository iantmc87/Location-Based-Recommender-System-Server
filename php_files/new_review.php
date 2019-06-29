<?php

/*
* Author - Ian McManus
* Date - 20/06/2019
* Version - 1.0.0
* Description - adds new review to the database
*/	
	
	#imports database connection file and variables
	require 'db_connect.php';
	global $connect, $reviewsTable, $userTable, $businessTable;

	#gets review data from the application
	$place = $_POST["title"];
	$user = $_POST['user_name'];
	$text = $_POST['text'];
	$stars = $_POST['rating'];
	$date = $_POST['date'];
	$name = $_POST['name'];
	
	#uploads data to the database
	$query = "INSERT INTO $reviewsTable (user_id, stars, name, business_id, date, text) VALUES ((SELECT u.user_id FROM $userTable u WHERE u.user_name = '$user'), $stars, '$name', (SELECT b.business_id FROM $businessTable b WHERE b.name = '$place'), '$date', '$text')";
	mysqli_query($connect, $query) or die (mysqli_error($connect));
	mysqli_close($connect);

?>
