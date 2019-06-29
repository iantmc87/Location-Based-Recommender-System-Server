<?php

/* 
* Author - Ian McManus
* Date - 20/01/2019
* Version - 1.0.0
* Description - Update database with user preferences
*/

	#imports database connection file and variables
	require 'db_connect.php';
	global $connect, $userTable;

	#gets data from the application
	$value = $_POST['value'];
	$user = $_POST['user_name'];
	$column = $_POST['column'];
	
	#updates database with the data
	$query = "UPDATE $userTable SET $column = '$value' WHERE user_name = '$user'";
	mysqli_query($connect, $query) or die (mysqli_error($connect));
	mysqli_close($connect);

?>
