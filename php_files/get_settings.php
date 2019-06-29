<?php

/*
* Author - Ian McManus
* Date - 20/06/2019
* Version - 1.0.0
* Description - gets system preference settings from database
*/

	#imports database connection file and variables
	require 'db_connect.php';
	global $connect, $userTable;

	#gets username from the application
	$user = $_POST['user_name'];

	#creates array to hold sql data
	$settings = array();

	#gets data from the database
	$sql = "SELECT categories, system, radius, alcohol, good_for_kids, good_for_groups, dogs_allowed, wheelchair_accessible, price_range FROM $userTable WHERE user_name = '$user'";
	$stmt = $connect->prepare($sql);
	$stmt->bind_param("s", $user);
	$stmt->execute();
	$stmt->bind_result($categories, $system, $radius, $alcohol, $good_for_kids, $good_for_groups, $dogs_allowed, $wheelchair_accessible, $price_range);

	#populates array with database data
	while($stmt->fetch()) {
		$temp = ['categories'=>$categories,			
			'system'=>$system,
			'radius'=>$radius,
			'alcohol'=>$alcohol,
			'good_for_kids'=>$good_for_kids,
			'good_for_groups'=>$good_for_groups,
			'dogs_allowed'=>$dogs_allowed,
			'wheelchair_accessible'=>$wheelchair_accessible,
			'price_range'=>$price_range
	];
	array_push($settings, $temp);
	}

	#sends data to the application
	header('Content-Type: application/json');
	echo json_encode(array('get_settings'=>$settings));

?> 
