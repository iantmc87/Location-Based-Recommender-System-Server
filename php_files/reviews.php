<?php

/*
* Author - Ian McManus
* Date - 20/06/2019
* Version - 1.0.0
* Description - gets review data from the database
*/	

	#imports database connection file and variables
	require 'db_connect.php';
	global $connect, $reviewsTable, $businessTable;

	#gets business name from application
	$business = $_POST['business'];	

	#creates array to hold sql data		
	$data = array();

	#gets data from the database
	$sql = "SELECT name, stars, date, text FROM $reviewsTable WHERE business_id IN (SELECT b.business_id FROM $businessTable b WHERE b.name = \"$business\") LIMIT 10";
	$stmt = $connect->prepare($sql);
	$stmt->bind_param("s", $business);
	$stmt->execute();
	$stmt->bind_result($name, $stars, $date, $text);
	
	#populates array with database data
	while($stmt->fetch()) {
		$temp = ['name'=>$name,
			'stars'=>$stars,
			'date'=>$date,
			'text'=>$text
		];
	array_push($data, $temp);
	}

	#sends data to the application
	header('Content-Type: application/json');
	echo json_encode(array("reviews"=>$data));
