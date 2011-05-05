<?php

@include_once("scaliendb.php");

ScalienClient::setTrace(TRUE);

$client = new ScalienClient(array("localhost:7080"));
$client->useDatabase("testdb");
$client->useTable("testtable");
print($client->set("UzXM3k7UGr", "UzXM3k7UGr"));
print($client->get("UzXM3k7UGr"));

?>
