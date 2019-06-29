<?php

/*
* Author - Ian McManus
* Date - 20/06/2019
* Version - 1.0.0
* Description - gets recommended businesses and location data from the database
*/

	#imports database connection file and variables
	include 'db_connect.php';
	global $connect, $business, $geolocation, $userTable;

	#gets data from the application
	$user = $_POST['user_name'];
	$latitude = $_POST['latitude'];
	$longitude = $_POST['longitude'];
	
	#creates array to hold sql data	
	$recommendations = array();

	#gets data from the database
	$sql = "SELECT b.name, SUBSTRING(b.categories, 1, 65), g.latitude, g.longitude, (((acos(sin(($latitude *pi()/180)) * sin((g.latitude*pi()/180)) + cos(($latitude*pi()/180)) *cos((g.latitude*pi()/180)) * cos((($longitude - g.longitude)*pi()/180))))*180/pi())*60*1.1515) as distance, b.stars FROM geolocation g LEFT JOIN business b ON b.postcode = g.postcode WHERE b.business_id IN (SELECT u.first_recommendation FROM $userTable u WHERE u.user_name = '$user') LIMIT 1";
	$stmt = $connect->prepare($sql);
	$stmt->bind_param("sss", $user, $latitude, $longitude);
	$stmt->execute();
	$stmt->bind_result($name, $categories, $latitude1, $longitude1, $distance, $average_stars);
	
	#populates the array with database data
	while($stmt->fetch()) {
		$temp = ['name'=>$name,
			'categories'=>$categories,
			'distance'=>$distance,
			'rating'=>$average_stars,
			'latitude'=>$latitude1,
			'longitude'=>$longitude1
	];
	array_push($recommendations, $temp);
	}

	#gets data from the database
	$sql = "SELECT b.name, SUBSTRING(b.categories,1,65), g.latitude, g.longitude, (((acos(sin(($latitude*pi()/180)) * sin((g.latitude*pi()/180)) + cos(($latitude*pi()/180)) *cos((g.latitude*pi()/180)) * cos((($longitude - g.longitude)*pi()/180))))*180/pi())*60*1.1515) as distance, b.stars FROM geolocation g LEFT JOIN business b ON b.postcode = g.postcode WHERE b.business_id IN (SELECT u.second_recommendation FROM $userTable u WHERE u.user_name = '$user') LIMIT 1";
	$stmt = $connect->prepare($sql);
	$stmt->bind_param("sss", $user, $latitude, $longitude);
	$stmt->execute();
	$stmt->bind_result($name, $categories, $latitude1, $longitude1, $distance, $average_stars);
	
	#populates the array with database data
	while($stmt->fetch()) {
		$temp = ['name'=>$name,
			'categories'=>$categories,
			'distance'=>$distance,
			'rating'=>$average_stars,
			'latitude'=>$latitude1,
			'longitude'=>$longitude1
	];
	array_push($recommendations, $temp);
	}

	#gets data from the database
	$sql = "SELECT b.name, SUBSTRING(b.categories,1,65), g.latitude, g.longitude, (((acos(sin(($latitude*pi()/180)) * sin((g.latitude*pi()/180)) + cos(($latitude*pi()/180)) *cos((g.latitude*pi()/180)) * cos((($longitude - g.longitude)*pi()/180))))*180/pi())*60*1.1515) as distance, b.stars FROM geolocation g LEFT JOIN business b ON b.postcode = g.postcode WHERE b.business_id IN (SELECT u.third_recommendation FROM $userTable u WHERE u.user_name = '$user') LIMIT 1";
	$stmt = $connect->prepare($sql);
	$stmt->bind_param("sss", $user, $latitude, $longitude);
	$stmt->execute();
	$stmt->bind_result($name, $categories, $latitude1, $longitude1, $distance, $average_stars);
	
	#populates the array with database data
	while($stmt->fetch()) {
		$temp = ['name'=>$name,
			'categories'=>$categories,
			'distance'=>$distance,
			'rating'=>$average_stars,
			'latitude'=>$latitude1,
			'longitude'=>$longitude1
	];
	array_push($recommendations, $temp);
	}

	#gets data from the database
	$sql = "SELECT b.name, SUBSTRING(b.categories,1,65), g.latitude, g.longitude, (((acos(sin(($latitude*pi()/180)) * sin((g.latitude*pi()/180)) + cos(($latitude*pi()/180)) *cos((g.latitude*pi()/180)) * cos((($longitude - g.longitude)*pi()/180))))*180/pi())*60*1.1515) as distance, b.stars FROM geolocation g LEFT JOIN business b ON b.postcode = g.postcode WHERE b.business_id IN (SELECT u.fourth_recommendation FROM $userTable u WHERE u.user_name = '$user') LIMIT 1";
	$stmt = $connect->prepare($sql);
	$stmt->bind_param("sss", $user, $latitude, $longitude);
	$stmt->execute();
	$stmt->bind_result($name, $categories, $latitude1, $longitude1, $distance, $average_stars);
	
	#populates the array with database data
	while($stmt->fetch()) {
		$temp = ['name'=>$name,
			'categories'=>$categories,
			'distance'=>$distance,
			'rating'=>$average_stars,
			'latitude'=>$latitude1,
			'longitude'=>$longitude1
	];
	array_push($recommendations, $temp);
	}

	#gets data from the database
	$sql = "SELECT b.name, SUBSTRING(b.categories,1,65), g.latitude, g.longitude, (((acos(sin(($latitude*pi()/180)) * sin((g.latitude*pi()/180)) + cos(($latitude*pi()/180)) *cos((g.latitude*pi()/180)) * cos((($longitude - g.longitude)*pi()/180))))*180/pi())*60*1.1515) as distance, b.stars FROM geolocation g LEFT JOIN business b ON b.postcode = g.postcode WHERE b.business_id IN (SELECT u.fifth_recommendation FROM $userTable u WHERE u.user_name = '$user') LIMIT 1";
	$stmt = $connect->prepare($sql);
	$stmt->bind_param("sss", $user, $latitude, $longitude);
	$stmt->execute();
	$stmt->bind_result($name, $categories, $latitude1, $longitude1, $distance, $average_stars);
	
	#populates the array with database data
	while($stmt->fetch()) {
		$temp = ['name'=>$name,
			'categories'=>$categories,
			'distance'=>$distance,
			'rating'=>$average_stars,
			'latitude'=>$latitude1,
			'longitude'=>$longitude1
	];
	array_push($recommendations, $temp);
	}

	#sends data to the application
	header('Content-Type: application/json');
	echo json_encode(array("recommendations"=>$recommendations));
		
?>	
