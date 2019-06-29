<?php
/*
* Author - Ian McManus
* Date - 20/06/2019
* Version - 1.0.0
* Description - gets postcodes for the autocomplete textbox
*/

	#imports database connection file and variables
	require 'db_connect.php';
	global $connect, $geolocationTable;

	#creates array to hold sql data
	$autocomplete = array();

	#gets data from the database
	$sql = "SELECT postcode FROM $geolocationTable";
	$stmt = $connect->prepare($sql);
	$stmt->execute();
	$stmt->bind_result($postcode);

	#populates array with database data
	while($stmt->fetch()) {
		$temp = ['postcode'=>$postcode
	];
	array_push($autocomplete, $temp);
	}

	#sends data to the application
	header('Content-Type: application/json');
	echo json_encode(array("autocomplete"=>$autocomplete));
?>
