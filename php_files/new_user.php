<?php

/*
* Author - Ian McManus
* Date - 20/06/2019
* Version - 1.0.0
* Description - adds new user to the database
*/

	#imports database connection file and variables
	require 'db_connect.php';
	global $connect, $userTable;
	
	#gets username from application
	$user = $_POST['user_name'];

	#uploads data to the database
	$query = "INSERT IGNORE INTO $userTable (user_name) VALUES ('$user')";
	mysqli_query($connect, $query) or die (mysqli_error($connect));
	mysqli_close($connect);

?>
