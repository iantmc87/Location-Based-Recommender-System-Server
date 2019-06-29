<?php
/*
* Author - Ian McManus
* Date - 20/06/2019
* Version - 1.0.0
* Description - gets business information
*/

	#imports database connection file and variables
	require 'db_connect.php';
	global $connect, $businessTable, $geolocationTable;

	#gets data from app
	$business = $_POST['business'];

	#creates array to hold sql data
	$info = array();

	#gets data from the database
	$sql = "SELECT b.name, b.categories, g.city, g.district, g.postcode FROM $businessTable b LEFT JOIN $geolocationTable g ON b.postcode = g.postcode WHERE b.name = \"$business\" LIMIT 1;";
	$stmt = $connect->prepare($sql);
	$stmt->bind_param("s", $business);
	$stmt->execute();
	$stmt->bind_result($name, $categories, $city, $district, $postcode);
	
	#populates array with database data
	while($stmt->fetch()) {
		$temp = ['name'=>$name,
			'categories'=>$categories,
			'city'=>$city,
			'district'=>$district,
			'postcode'=>$postcode
	];
	array_push($info, $temp);
	}

	#sends data to the application
	header('Content-Type: application/json');
	echo json_encode(array('info'=>$info));

?>
