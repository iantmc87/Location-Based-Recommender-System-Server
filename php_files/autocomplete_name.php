<?php
/*
* Author - Ian McManus
* Date - 20/06/2019
* Version - 1.0.0
* Description - gets business names for the autocomplete textbox
*/

	#imports database connection file and variables
	require 'db_connect.php';
	global $connect, $businessTable;
	
	#creates array to hold sql data
	$autocomplete = array();

	#gets data from the database
	$sql = "SELECT name FROM $businessTable";
	$stmt = $connect->prepare($sql);
	$stmt->execute();
	$stmt->bind_result($name);

	#populates array with database data
	while($stmt->fetch()) {
		$temp = ['title'=>$name
	];
	array_push($autocomplete, $temp);
	}

	#sends data to the application
	header('Content-Type: application/json; charset=utf-8');
	echo json_encode(array("autocomplete"=>$autocomplete));
	 
?>
