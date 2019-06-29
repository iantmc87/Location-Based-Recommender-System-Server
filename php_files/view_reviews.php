<?php
	
/*
* Author - Ian McManus
* Date - 20/06/2019
* Version - 1.0.0
* Description - gets user reviews from the database
*/

	#imports database connection file and variables
	include 'db_connect.php';
	global $connect, $reviewsTable, $businessTable, $userTable;

	#gets data from the application
	$user = $_POST['user_name'];	
		
	#creates the array to hold the sql data
	$data = array();

	#gets data from the database
	$query = "SELECT b.name, r.stars, r.date, r.text FROM $reviewsTable r INNER JOIN $businessTable b ON b.business_id = r.business_id WHERE r.user_id IN (SELECT u.user_id FROM $userTable u WHERE u.user_name = '$user')";
	$stmt = $connect->prepare($query);
	$stmt->bind_param("ssss", $reviewsTable, $businessTable, $userTable, $user);
	$stmt->execute();
	$stmt->bind_result($name, $stars, $date, $text);
	
	#populates array with database data
	while($stmt->fetch()) {
		$temp =['name'=>$name,
			'stars'=>$stars,
			'date'=>$date,
			'text'=>$text
		];
	array_push($data, $temp);
	}

	#sends data to the application
	header('Content-Type: application/json');
	echo json_encode(array("reviews"=>$data));

?>
