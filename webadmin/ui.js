var $ = scaliendb.util.elem;
var lastConfigState = null;

function contains(arr, obj)
{
	var i = arr.length;
	while (i--)
	{
		if (arr[i] === obj)
			return true;
	}
	return false;
}

function init()
{	
	scaliendb.disconnect();
	setConnectionLocation();
	setDeveloperMode();

	$("controller").textContent = "Not connected...";
	$("clusterState").textContent = "Unable to connect!";
	$("clusterState").className = "status-message critical";

	$("shardservers").innerHTML = "";

	scaliendb.util.removeElement("quorums");
	var quorumsDiv = document.createElement("div");
	quorumsDiv.setAttribute("id", "quorums");
	$("tabPageQuorums").appendChild(quorumsDiv);

	scaliendb.util.removeElement("databases");
	var databasesDiv = document.createElement("div");
	databasesDiv.setAttribute("id", "databases");
	$("tabPageSchema").appendChild(databasesDiv);
}

function onLoad()
{
	scaliendb.onDisconnect = onDisconnect;
	init();
	$("loginContainer").style.display = "block";
	$("mainContainer").style.display = "none";
	$("unregisterShardServerContainer").style.display = "none";
	$("createQuorumContainer").style.display = "none";
	$("renameQuorumContainer").style.display = "none";
	$("deleteQuorumContainer").style.display = "none";
	$("addNodeContainer").style.display = "none";
	$("removeNodeContainer").style.display = "none";
	$("createDatabaseContainer").style.display = "none";
	$("renameDatabaseContainer").style.display = "none";
	$("deleteDatabaseContainer").style.display = "none";
	$("createTableContainer").style.display = "none";
	$("renameTableContainer").style.display = "none";
	$("truncateTableContainer").style.display = "none";
	$("deleteTableContainer").style.display = "none";
	$("splitShardContainer").style.display = "none";
	$("migrateShardContainer").style.display = "none";
	$("errorContainer").style.display = "none";
	$("loginCluster").focus();
	removeOutline();
	
	activateDashboardTab();
	hideDialog = function() {}
	
	document.onkeydown = function(e) { if (getKeycode(e) == 27) hideDialog(); }
}

var md = false;
function onMouseDown()
{
	md = true;	
}

function onMouseUp()
{
	md = false;
}

function validateIfNotEmpty(node, button)
{
	if (node.value.length > 0)
		button.disabled = false;
	else
		button.disabled = true;
}

function logout()
{
	clearTimeout(timer);
	onLoad();
	$("loginCluster").select();
}

function setDeveloperMode()
{
	var match = /[\?&]dev($|&)/.exec(window.location);
	if (match == null)
		scaliendb.developer = false;
	else
		scaliendb.developer = true;
}
                                      
function setConnectionLocation()
{
	var match = /^\w+:\/\/([\w.]+:[0-9]+)\/.*$/.exec(window.location);
	if (match == null)
		return;
	controller = match[1];
	if (!scaliendb.util.endsWith(controller, "/json/"))
		controller = controller + "/json/";
	$("loginCluster").value = scaliendb.controller = controller;
}

function connect()
{
	$("loginContainer").style.display = "none";
	$("mainContainer").style.display = "block";

	var controller = $("loginCluster").value;

	var direct = false;
	if (scaliendb.util.startsWith(controller, "direct://"))
	{
		controller = controller.replace("direct://", "");
		direct = true;     
	}
	
	// HACK Windows 7 IPv6 localhost resolv bug fix
	if (!scaliendb.util.startsWith(controller, "http://"))
	{
		if (scaliendb.util.startsWith(controller, "localhost"))
			controller = controller.replace(/localhost/i, "127.0.0.1");
		controller = "http://" + controller;
	}
	else
		controller = controller.replace(/http:\/\/localhost/i, "http://127.0.0.1");
	
	if (!scaliendb.util.endsWith(controller, "/json/"))
		controller = controller + "/json/";
	if (controller !== scaliendb.controller)
		scaliendb.controller = controller;

	if (direct)
		updateConfigState();
    else
		findMaster();
}

tabs = ["Dashboard", "Quorums", "Schema", "Migration"];

function activateTab(name)
{
	for (var t in tabs)
	{
		var tab = tabs[t];
		if (tab == name)
		{
			$("tabHead" + tab).className = "tab-head tab-head-active";
			$("tabPage" + tab).className = "tab-page tab-page-active";
		}
		else
		{
			$("tabHead" + tab).className = "tab-head tab-head-inactive";
			$("tabPage" + tab).className = "tab-page tab-page-inactive";			
		}
	}
}

function activateDashboardTab()
{
	activateTab("Dashboard");
}

function activateQuorumsTab()
{
	activateTab("Quorums");
}

function activateSchemaTab()
{
	activateTab("Schema");
}

function activateMigrationTab()
{
	activateTab("Migration");
}

var unregisterShardServerID;
function showUnregisterShardServer(nodeID)
{
	$("mainContainer").style.display = "none";
	$("unregisterShardServerContainer").style.display = "block";
	unregisterShardServerID = nodeID;
	hideDialog = hideUnregisterShardServer;
}

var tableShardsVisible = new Array();
function showhideShardsDiv(tableID)
{
	if (tableShardsVisible[tableID])
	{
		tableShardsVisible[tableID] = false;
		$("shards_" + tableID).style.display = "none";
		$("showhideShardsButton_" + tableID).innerHTML = "show shards";
	}
	else
	{
		tableShardsVisible[tableID] = true;
		$("shards_" + tableID).style.display = "block";
		$("showhideShardsButton_" + tableID).innerHTML = "hide shards";
	}
}

function showCreateQuorum()
{
	$("mainContainer").style.display = "none";
	$("createQuorumContainer").style.display = "block";
	$("createQuorumShardServers").focus();
	hideDialog = hideCreateQuorum;
}

var renameQuorumID;
function showRenameQuorum(quorumID)
{
	$("mainContainer").style.display = "none";
	$("renameQuorumContainer").style.display = "block";
	renameQuorumID = quorumID;
	hideDialog = hideRenameQuorum;
}

var deleteQuorumID;
function showDeleteQuorum(quorumID)
{
	$("mainContainer").style.display = "none";
	$("deleteQuorumContainer").style.display = "block";
	deleteQuorumID = quorumID;
	hideDialog = hideDeleteQuorum;
}

var addNodeQuorumID;
function showAddNode(quorumID)
{
	$("mainContainer").style.display = "none";
	$("addNodeContainer").style.display = "block";

	populateSelector("addNodeShardServerSelector", shardServersNotInQuorum(quorumID));

	addNodeQuorumID = quorumID;
	hideDialog = hideAddNode;
}

var removeNodeQuorumID;
function showRemoveNode(quorumID)
{
	$("mainContainer").style.display = "none";
	$("removeNodeContainer").style.display = "block";

	populateSelector("removeNodeShardServerSelector", shardServersInQuorum(quorumID));

	removeNodeQuorumID = quorumID;
	hideDialog = hideRemoveNode;
}

function showCreateDatabase()
{
	$("mainContainer").style.display = "none";
	$("createDatabaseContainer").style.display = "block";

	var button = $("createDatabaseButton");
	var name = $("createDatabaseName");
	name.focus();
	name.select();
	name.onchange = function() { validateIfNotEmpty(name, button); }
	name.onkeyup = function() { validateIfNotEmpty(name, button); }

	validateIfNotEmpty(name, button);

	hideDialog = hideCreateDatabase;
}

var renameDatabaseID;
function showRenameDatabase(databaseID, databaseName)
{
	$("mainContainer").style.display = "none";
	$("renameDatabaseContainer").style.display = "block";

	var button = $("renameDatabaseButton");
	var name = $("renameDatabaseName");
	name.value = databaseName;
	name.focus();
	name.select();
	name.onchange = function() { validateIfNotEmpty(name, button); }
	name.onkeyup = function() { validateIfNotEmpty(name, button); }

	validateIfNotEmpty(name, button);
	
	renameDatabaseID = databaseID;
	hideDialog = hideRenameDatabase;
}

var deleteDatabaseID;
function showDeleteDatabase(databaseID)
{
	$("mainContainer").style.display = "none";
	$("deleteDatabaseContainer").style.display = "block";
	deleteDatabaseID = databaseID;
	hideDialog = hideDeleteDatabase;
}

var createTableDatabaseID;
function showCreateTable(databaseID, databaseName)
{
	$("mainContainer").style.display = "none";
	$("createTableContainer").style.display = "block";

	var button = $("createTableButton");
	var name = $("createTableName");
	name.focus();
	name.select();
	name.onchange = function() { validateIfNotEmpty(name, button); }
	name.onkeyup = function() { validateIfNotEmpty(name, button); }

	validateIfNotEmpty(name, button);
	populateQuorumSelector("createTableQuorumSelector");
	
	createTableDatabaseID = databaseID;
	hideDialog = hideCreateTable;
}

var renameTableID;
function showRenameTable(tableID, tableName)
{
	$("mainContainer").style.display = "none";
	$("renameTableContainer").style.display = "block";

	var button = $("renameTableButton");
	var name = $("renameTableName");
	name.value = tableName;
	name.focus();
	name.select();
	name.onchange = function() { validateIfNotEmpty(name, button); }
	name.onkeyup = function() { validateIfNotEmpty(name, button); }

	validateIfNotEmpty(name, button);

	renameTableID = tableID;
	hideDialog = hideRenameTable;
}

var truncateTableID;
function showTruncateTable(tableID)
{
	$("mainContainer").style.display = "none";
	$("truncateTableContainer").style.display = "block";
	truncateTableID = tableID;
	hideDialog = hideTruncateTable;
}

var deleteTableID;
function showDeleteTable(tableID)
{
	$("mainContainer").style.display = "none";
	$("deleteTableContainer").style.display = "block";
	deleteTableID = tableID;
	hideDialog = hideDeleteTable;
}

var splitShardID;
function showSplitShard(shardID)
{
	$("mainContainer").style.display = "none";
	$("splitShardContainer").style.display = "block";
	$("splitShardKey").focus();
	$("splitShardKey").select();	
	splitShardID = shardID;
	hideDialog = hideSplitShard;
}

var migrateShardID;
function showMigrateShard(shardID)
{
	$("mainContainer").style.display = "none";
	$("migrateShardContainer").style.display = "block";
	// $("migrateShardQuorum").focus();
	// $("migrateShardQuorum").select();
	
	var numQuorums = populateQuorumSelector("migrateShardQuorumSelector", shardID);
	if (numQuorums == 0)
	{
		$("migrateShardText").innerHTML = "No available quorums!";
		$("migrateShardButton").innerHTML = "OK";
		$("migrateShardQuorumSelector").style.visibility = "hidden";
	}
	else
	{
		$("migrateShardText").innerHTML = "Migrate to quorum:";
		$("migrateShardButton").innerHTML = "Migrate";
		$("migrateShardQuorumSelector").style.visibility = "visible";
	}
	
	migrateShardID = shardID;
	hideDialog = hideMigrateShard;
}

function showError(title, text)
{
	$("mainContainer").style.display = "none";
	$("errorContainer").style.display = "block";
	$("errorTitle").innerHTML = title;
	$("errorText").innerHTML = text;
	hideDialog = hideError;
}

function populateQuorumSelector(name, shardID)
{
	var selector = $(name);
	if (selector == null || !lastConfigState.hasOwnProperty("quorums"))
		return 0;

	// delete child nodes
	while (selector.hasChildNodes()) {
	    selector.removeChild(selector.lastChild);
	}
	
	// populate quorumIDs
	for (var q in lastConfigState["quorums"])
	{
		var quorum = lastConfigState["quorums"][q];
		if (!quorum.hasOwnProperty("quorumID"))
			continue;

		// shardID is optional
	    if (shardID != undefined)
		{
			var currentQuorum = false;
			for (var qs in quorum["shards"])
			{
				var quorumShardID = quorum["shards"][qs];
				if (quorumShardID == shardID)
				{
					currentQuorum = true;
					break;
				}
			}
			if (currentQuorum)
				continue;
		}
		var quorumID = quorum["quorumID"];
		var node = document.createElement("option");
		node.innerHTML = quorumID;
		node.setAttribute("value", "quorum_" + quorumID);
		selector.appendChild(node);
	}

	// set the first selected
	if (selector.options.length > 0)
		selector.options[0].selected = true;
	
	return selector.options.length;
}

function shardServersNotInQuorum(quorumID)
{
	var shardServers = [];
	// populate quorumIDs
	for (var s in lastConfigState["shardServers"])
	{
		var shardServer = lastConfigState["shardServers"][s];
		if (!shardServer.hasOwnProperty("quorumInfos"))
			continue;

	    if (quorumID != undefined)
		{
			var found = false;
			for (var qi in shardServer["quorumInfos"])
			{
				var quorumInfo = shardServer["quorumInfos"][qi];
				if (quorumInfo["quorumID"] == quorumID)
				{
					// skip shardserver
					found = true;
					break;
				}
			}
			if (found)
				continue;
		}
		shardServers.push(shardServer["nodeID"]);
	}

	return shardServers;
}

function shardServersInQuorum(quorumID)
{
	var shardServers = [];
	// populate quorumIDs
	for (var s in lastConfigState["shardServers"])
	{
		var shardServer = lastConfigState["shardServers"][s];
		if (!shardServer.hasOwnProperty("quorumInfos"))
			continue;

	    if (quorumID != undefined)
		{
			var found = false;
			for (var qi in shardServer["quorumInfos"])
			{
				var quorumInfo = shardServer["quorumInfos"][qi];
				if (quorumInfo["quorumID"] == quorumID)
				{
					// skip shardserver
					shardServers.push(shardServer["nodeID"]);
					break;
				}
			}
		}
	}

	return shardServers;
}

function populateSelector(name, list)
{
	var selector = $(name);	
	if (selector == null || !lastConfigState.hasOwnProperty("quorums"))
		return 0;

	// delete child nodes
	while (selector.hasChildNodes()) {
	    selector.removeChild(selector.lastChild);
	}		
	
	for (var l in list)
	{
		var node = document.createElement("option");
		node.innerHTML = list[l];
		node.setAttribute("value", list[l]);
		selector.appendChild(node);
	}

	// set the first selected
	if (selector.options.length > 0)
		selector.options[0].selected = true;
	
	return selector.options.length;	
}

function hideUnregisterShardServer()
{
	$("unregisterShardServerContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideCreateQuorum()
{
	$("createQuorumContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideRenameQuorum()
{
	$("renameQuorumContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideDeleteQuorum()
{
	$("deleteQuorumContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideAddNode()
{
	$("addNodeContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideRemoveNode()
{
	$("removeNodeContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideCreateDatabase()
{
	$("createDatabaseContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideRenameDatabase()
{
	$("renameDatabaseContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideDeleteDatabase()
{
	$("deleteDatabaseContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideCreateTable()
{
	$("createTableContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideRenameTable()
{
	$("renameTableContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideTruncateTable()
{
	$("truncateTableContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideDeleteTable()
{
	$("deleteTableContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideSplitShard()
{
	$("splitShardContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideMigrateShard()
{
	$("migrateShardContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function hideError()
{
	$("errorContainer").style.display = "none";
	$("mainContainer").style.display = "block";
}

function unregisterShardServer()
{
	hideUnregisterShardServer();
	scaliendb.onResponse = onResponse;
	scaliendb.unregisterShardServer(unregisterShardServerID);
}

function createQuorum()
{
	hideCreateQuorum();

	var name = $("createQuorumName").value;
	var nodes = $("createQuorumShardServers").value;
	nodes = scaliendb.util.removeSpaces(nodes);
	scaliendb.onResponse = onResponse;
	scaliendb.createQuorum(name, nodes);
}

function renameQuorum()
{
	hideRenameQuorum();
	var name = $("renameQuorumName").value;
	scaliendb.onResponse = onResponse;
	scaliendb.renameQuorum(renameQuorumID, name);
}

function deleteQuorum()
{
	hideDeleteQuorum();
	scaliendb.onResponse = onResponse;
	scaliendb.deleteQuorum(deleteQuorumID);
}

function addNode()
{
	hideAddNode();

	// var nodeID = $("addNodeShardServer").value;
	var selector = $("addNodeShardServerSelector");
	var nodeID = selector.options[selector.selectedIndex].text;
	nodeID = scaliendb.util.removeSpaces(nodeID);
	scaliendb.onResponse = onResponse;
	scaliendb.addNode(addNodeQuorumID, nodeID);
}

function removeNode()
{
	hideRemoveNode();

	// var nodeID = $("removeNodeShardServer").value;
	var selector = $("removeNodeShardServerSelector");
	var nodeID = selector.options[selector.selectedIndex].text;
	nodeID = scaliendb.util.removeSpaces(nodeID);
	scaliendb.onResponse = onResponse;
	scaliendb.removeNode(removeNodeQuorumID, nodeID);
}

function activateNode(quorumID, nodeID)
{
	scaliendb.onResponse = onResponse;
	scaliendb.activateNode(quorumID, nodeID);
}

function createDatabase()
{
	hideCreateDatabase();
	var name = $("createDatabaseName").value;
	name = scaliendb.util.removeSpaces(name);
	scaliendb.onResponse = onResponse;
	scaliendb.createDatabase(name);
}

function renameDatabase()
{
	hideRenameDatabase();
	var name = $("renameDatabaseName").value;
	name = scaliendb.util.removeSpaces(name);
	scaliendb.onResponse = onResponse;
	scaliendb.renameDatabase(renameDatabaseID, name);
}

function deleteDatabase()
{
	hideDeleteDatabase();
	scaliendb.onResponse = onResponse;
	scaliendb.deleteDatabase(deleteDatabaseID);
}

function createTable()
{
	hideCreateTable();
	var name = $("createTableName").value;
	name = scaliendb.util.removeSpaces(name);
	// var quorumID = $("createTableQuorum").value;
	var selector = $("createTableQuorumSelector");
	var quorumID = selector.options[selector.selectedIndex].text;
	quorumID = scaliendb.util.removeSpaces(quorumID);
	scaliendb.onResponse = onResponse;
	scaliendb.createTable(createTableDatabaseID, quorumID, name);
}

function renameTable()
{
	hideRenameTable();
	var name = $("renameTableName").value;
	name = scaliendb.util.removeSpaces(name);
	scaliendb.onResponse = onResponse;
	scaliendb.renameTable(renameTableID, name);
}

function truncateTable()
{
	hideTruncateTable();
	scaliendb.onResponse = onResponse;
	scaliendb.truncateTable(truncateTableID);
}

function deleteTable()
{
	hideDeleteTable();
	scaliendb.onResponse = onResponse;
	scaliendb.deleteTable(deleteTableID);
}

function freezeTable(tableID)
{
	scaliendb.onResponse = onResponse;
	scaliendb.freezeTable(tableID);
}

function unfreezeTable(tableID)
{
	scaliendb.onResponse = onResponse;
	scaliendb.unfreezeTable(tableID);
}

function freezeDatabase(databaseID)
{
	scaliendb.onResponse = onResponse;
	scaliendb.freezeDatabase(databaseID);
}

function unfreezeDatabase(databaseID)
{
	scaliendb.onResponse = onResponse;
	scaliendb.unfreezeDatabase(databaseID);
}

function splitShard()
{
	hideSplitShard();
	var key = $("splitShardKey").value;
	scaliendb.onResponse = onResponse;
	scaliendb.splitShard(splitShardID, key);
}

function migrateShard()
{
	hideMigrateShard();
	// var quorumID = $("migrateShardQuorum").value;
	var selector = $("migrateShardQuorumSelector");
	var quorumID = selector.options[selector.selectedIndex].text;
	quorumID = scaliendb.util.removeSpaces(quorumID);
	scaliendb.onResponse = onResponse;
	scaliendb.migrateShard(migrateShardID, quorumID);
}

function onResponse(json)
{
	if (json.hasOwnProperty("response") && json["response"] == "FAILED")
		showError("Error", "Something went wrong!");
	updateConfigState();
}

function onDisconnect()
{
	init();
	createDashboard({});
	clearTimeout(timer);
	timer = setTimeout("connect()", 1000);	
}

function findMaster()
{
	scaliendb.findMaster(onFindMaster);
}

function onFindMaster(obj)
{
	if (obj["response"] == "NOSERVICE" || obj["response"] == "0.0.0.0:0")
	{
		setTimeout("findMaster()", 1000);
		return;
	}
	
	var controller = obj["response"];
	if (!scaliendb.util.startsWith(controller, "http://"))
		controller = "http://" + controller;
	if (!scaliendb.util.endsWith("controller", "/json/"))
		controller = controller + "/json/";
	if (controller !== scaliendb.controller)
		scaliendb.controller = controller;

	updateConfigState();
}

function updateConfigState()
{
	scaliendb.getConfigState(onConfigState);
}

var timer;
function onConfigState(configState)
{
	lastConfigState = configState;
	$("controller").textContent = "Connected to " + scaliendb.controller;

	createDashboard(configState);
	
	for (var key in configState)
	{
		if (key == "quorums")
			createQuorumDivs(configState, configState[key]);
		else if (key == "databases")
			createDatabaseDivs(configState, configState[key]);
		else if (key == "shardServers")
			createMigrationDivs(configState, configState[key]);
	}
	
	$("clusterState").textContent = "The ScalienDB cluster is " + scaliendb.getClusterState(configState);
	$("clusterState").className = "status-message " + scaliendb.getClusterState(configState);
	
	// clearTimeout(timer);
	// timer = setTimeout("onTimeout()", 1000);
	scaliendb.timeout = 60 * 1000;
	scaliendb.pollConfigState(onConfigState, configState["paxosID"]);
}

function onTimeout()
{
	if (!md)
		updateConfigState();
}

function createDashboard(configState)
{
	var numDatabases = 0;
	var numDatabasesPrefixText = "is ";
	var numDatabasesText = "";
	var numTables = 0;
	var numTablesText = "";
	var maxTables = 0;
	var maxTablesText = "";
	var minTables = 999999999;
	var minTablesText = "";
	var avgTables = 0;
	var avgTablesText = "";
	var numShards = 0;
	var numShardsText = "";
	var maxShards = 0;
	var maxShardsText = "";
	var minShards = 999999999;
	var minShardsText = "";
	var avgShards = 0;
	var avgShardsText = "";
	var numShardServers = 0;
	var numShardServersText = "";
	var numQuorums = 0;
	var numQuorumsText = "";
	var shardServerIDs = new Array();
	var controllerIDs = new Array();
	var cardinality = scaliendb.util.cardinality;
	for (var key in configState)
	{
		if (key == "quorums")
		{
			var quorums = configState[key];
			numQuorums = quorums.length;
		}
		else if (key == "databases")
		{
			var databases = configState[key];  
			numDatabases = databases.length;
			if (numDatabases == 1)
				numDatabasesPrefixText = "is ";
			else
				numDatabasesPrefixText = "are ";
			for (var database in databases)
			{
				var db = databases[database];
				num = db["tables"].length;
				if (num < minTables)
					minTables = num;
				if (num > maxTables)
					maxTables = num;
			}
		}
		else if (key == "tables")
		{
			var tables = configState[key];
			numTables = tables.length;
			numTablesText = cardinality(numTables, "table");
			for (var table in tables)
			{
				var tbl = tables[table];
				var num = tbl["shards"].length;
				if (num < minShards)
					minShards = num;
				if (num > maxShards)
					maxShards = num;
			}
		}
		else if (key == "shards")
		{
			var shards = configState[key]; 
			numShards = shards.length;
		}
		else if (key == "controllers")
		{
			var controllers = configState[key];   
			numControllers = controllers.length;
			for (var c in controllers)
			{
				var controller = controllers[c];
				controllerIDs.push(controller["nodeID"]);
			}
		}
		else if (key == "shardServers")
		{
			var shardServers = configState[key];   
			numShardServers = shardServers.length;
			for (var ss in shardServers)
			{
				var shardServer = shardServers[ss];
				shardServerIDs.push(shardServer["nodeID"]);
			}
		}
	}
	numQuorumsText = cardinality(numQuorums, "quorum");
	numDatabasesText = cardinality(numDatabases, "database");
	minTablesText = cardinality(minTables, "table");
	maxTablesText = cardinality(maxTables, "table");
	minShardsText = cardinality(minShards, "shard");
	maxShardsText = cardinality(maxShards, "shard");
	numShardsText = cardinality(numShards, "shard");
	numShardServersText = cardinality(numShardServers, "shard server");
	avgTables = Math.round(numTables / numDatabases * 10) / 10;
	avgTablesText = cardinality(avgTables, "table");
	avgShards = Math.round(numShards / numTables * 10) / 10;
	avgShardsText = cardinality(avgShards, "shard")
	
	$("numDatabasesPrefix").textContent = numDatabasesPrefixText;
	$("numDatabases").textContent = numDatabasesText;
	$("numTables").textContent = numTablesText;
	$("numShards").textContent = numShardsText;
	$("numQuorums").textContent = numQuorumsText;
	$("numShardServers").textContent = numShardServersText;
	

	$("minTables").textContent = minTablesText;
	$("maxTables").textContent = maxTablesText;
	$("avgTables").textContent = avgTablesText;

	$("minShards").textContent = minShardsText;
	$("maxShards").textContent = maxShardsText;
	$("avgShards").textContent = avgShardsText;
	
	if (numDatabases == 0 || numTables == 0)
		$("dashboardStats").style.display = "none";
	else
		$("dashboardStats").style.display = "inline";
	
	scaliendb.util.clear($("shardservers"));

	var html = '';
	for (var i in controllerIDs)
	{
		nodeID = controllerIDs[i];
		controller = scaliendb.getController(configState, nodeID);
		var nodeString = nodeID + ' [' + controller["endpoint"] + ']';
		if (controller["isConnected"])
		{
		    if (configState["master"] == nodeID)
			    html += ' <span class="master healthy shardserver-number">' + nodeString + '</span> ';
			else
			    html += ' <span class="healthy shardserver-number">' + nodeString + '</span> ';
		}
		else
			html += ' <span class="no-heartbeat shardserver-number">' + nodeString + '</span> ';
	}
	$("configservers").innerHTML = html;

	var html = '';
	for (var i in shardServerIDs)
	{
		nodeID = shardServerIDs[i];
		shardServer = scaliendb.getShardServer(configState, nodeID);
		var nodeString = nodeID + ' [' + shardServer["endpoint"] + '] &nbsp; <a title="Remove from cluster" class="no-line" style="color:gray;" href="javascript:showUnregisterShardServer(' + nodeID + ')">X</a>';
		if (shardServer["hasHeartbeat"])
			html += ' <span class="healthy shardserver-number">' + nodeString + '</span> ';
		else
			html += ' <span class="no-heartbeat shardserver-number">' + nodeString + '</span> ';
	}
	$("shardservers").innerHTML = html;
}

function createQuorumDivs(configState, quorums)
{
	scaliendb.util.removeElement("quorums");
	var quorumsDiv = document.createElement("div");
	quorumsDiv.setAttribute("id", "quorums");
	
	for (var quorum in quorums)
	{
		var q = quorums[quorum];
		quorumDiv = createQuorumDiv(configState, q);
		quorumsDiv.appendChild(quorumDiv);
	}
	
	$("tabPageQuorums").appendChild(quorumsDiv);
}

function createQuorumDiv(configState, quorum)
{
	var state;
	var primaryID = null;
	explanation = "";
	if (quorum["hasPrimary"] == true)
	{
		if (quorum["inactiveNodes"].length == 0)
			state = "healthy";
		else
			state = "unhealthy";
		primaryID = quorum["primaryID"];
	}
	else
		state = "critical";
	
	var paxosID = "unknown";
	if (quorum["paxosID"] > 0)
		paxosID = quorum["paxosID"];
	
	var html =
	'																									\
	<table class="quorum ' + state + '">																\
		<tr>																							\
			<td class="quorum-head">																	\
				<span class="quorum-head">' + quorum["name"] + '<br/>(' + scaliendb.getQuorumState(configState, quorum["quorumID"]) + ')</span>						\
			</td>																						\
			<td>																						\
				<br/>																					\
				Quorum number: ' + quorum["quorumID"] + '<br/>													\
				Shardservers: 																			\
	';
	quorum["activeNodes"].sort();
	var catchupText = "";
	for (var i in quorum["activeNodes"])
	{
		var nodeID = quorum["activeNodes"][i];
		var shardServer = scaliendb.getShardServer(configState, nodeID);
		var priority = 1;
		for (var qp in shardServer["quorumPriorities"])
		{
			if (shardServer["quorumPriorities"][qp]["quorumID"] == quorum["quorumID"])
				priority = shardServer["quorumPriorities"][qp]["priority"]
		}
		var quorumInfo = scaliendb.getQuorumInfo(configState, nodeID, quorum["quorumID"]);
		var infoText = " [prio = " + priority + ", ";
		if (quorumInfo != null)
		{
			if (paxosID !== "unknown")
				infoText += " repl = " + paxosID + "]";
			else
				infoText += " repl = " + quorumInfo["paxosID"] + "]";
			if (quorumInfo["isSendingCatchup"])
				catchupText += "Shard server " + nodeID + " is sending catchup: " + scaliendb.util.humanBytes(quorumInfo["catchupBytesSent"]) + "/" + scaliendb.util.humanBytes(quorumInfo["catchupBytesTotal"]) + " (" + scaliendb.util.humanBytes(quorumInfo["catchupThroughput"]) + "/s)";
		}
		if (nodeID == primaryID)
		{
		 	html += ' <span class="shardserver-number primary ' + (shardServer["hasHeartbeat"] ? "healthy" : "no-heartbeat") + '"><b>' + nodeID + infoText + '</b></span> ';
			explanation = "The quorum has a primary (" + primaryID + "), it is writable. ";
		}
		else
		{
	 		html += ' <span class="shardserver-number ' + (shardServer["hasHeartbeat"] ? "healthy" : "no-heartbeat") + '">' + nodeID + infoText + '</span> ';
		}
	}
	if (primaryID == null)
		explanation += "The quorum has no primary, it is not writable. ";
	if (quorum["inactiveNodes"].length > 0 && primaryID != null)
		explanation += "The quorum has inactive nodes. These can be brought back into the quorum (once they are up and running and catchup is complete) by clicking them above. ";
	quorum["inactiveNodes"].sort();
	for (var i in quorum["inactiveNodes"])
	{
		var nodeID = quorum["inactiveNodes"][i];
		var shardServer = scaliendb.getShardServer(configState, nodeID);
		var priority = 1;
		for (var qp in shardServer["quorumPriorities"])
		{
			if (shardServer["quorumPriorities"][qp]["quorumID"] == quorum["quorumID"])
				priority = shardServer["quorumPriorities"][qp]["priority"]
		}
		var quorumInfo = scaliendb.getQuorumInfo(configState, nodeID, quorum["quorumID"]);
		var infoText = " [prio = " + priority + ", ";
		if (quorumInfo != null)
			infoText += " repl = " + quorumInfo["paxosID"] + "]";
		if (shardServer["hasHeartbeat"] && primaryID != null)
			html += ' <a class="no-line" style="color:black" title="Activate shard server" href="javascript:activateNode(' + quorum["quorumID"] + ", " + nodeID + ')"><span class="shardserver-number healthy">' + nodeID + infoText + ' (click to activate)</span></a> ';
		else
			html += ' <span class="shardserver-number ' + (shardServer["hasHeartbeat"] ? "healthy" : "no-heartbeat") + '">' + nodeID + infoText + '</span> ';
	}
	html +=
	'			<br/>																					\
				Shards: 																				\
	';
	quorum["shards"].sort();
	for (var i in quorum["shards"])
	{
		shardID = quorum["shards"][i]
		html += ' <span class="shard-number">' + shardID + '</span> ';
	}

	var addNode = "";
	if (shardServersNotInQuorum(quorum["quorumID"]).length > 0)
	{
		addNode = '<a class="no-line" href="javascript:showAddNode(' + quorum["quorumID"] +
		')"><span class="create-button">add server</span></a><br/><br/>';
	}	

	var removeNode = "";
	if (shardServersInQuorum(quorum["quorumID"]).length > 1)
	{
		removeNode = '<a class="no-line" href="javascript:showRemoveNode(' + quorum["quorumID"] +
		')"><span class="modify-button">remove server</span></a><br/><br/>';
	}
	html +=
	'																									\
				<br/>																					\
				Replication round: ' + paxosID + '<br/>													\
				<!-- Size: 0GB -->																		\
				<div class="explanation">Explanation: ' + explanation + '<br/>' + catchupText + '</span>\
			</td>																						\
			<td class="table-actions">' +
				addNode +
				removeNode +
				'<a class="no-line" href="javascript:showRenameQuorum(' + quorum["quorumID"] +
				')"><span class="modify-button">rename quorum</span></a><br/><br/>  					\
				 <a class="no-line" href="javascript:showDeleteQuorum(' + quorum["quorumID"] +
				')"><span class="delete-button">delete quorum</span></a>								\
			</td>																						\
		</tr>																							\
	</table>																							\
	';
	
	var div = document.createElement("div");
	div.setAttribute("class", "quorum " + state);
	div.innerHTML = html;
	return div;
}

function createDatabaseDivs(configState, databases)
{
	scaliendb.util.removeElement("databases");
	var databasesDiv = document.createElement("div");
	databasesDiv.setAttribute("id", "databases");
	
	for (var database in databases)
	{
		var db = databases[database];
		databaseDiv = createDatabaseDiv(configState, db);
		databasesDiv.appendChild(databaseDiv);
	}
	
	$("tabPageSchema").appendChild(databasesDiv);
}

function createDatabaseDiv(configState, database)
{
	var html =
	'																									\
	<span class="database-head">Listing tables for database: <span class="database-name">' + database["name"] + '</span></span>		\
	 - <a class="no-line" href="javascript:showDeleteDatabase(\'' + database["databaseID"] + '\')">		\
	<span class="delete-button">delete database</span></a>												\
	 - <a class="no-line" href="javascript:showRenameDatabase(\'' +
	database["databaseID"] + '\', \'' + database["name"] + '\')">										\
	<span class="modify-button">rename database</span></a>												\
	 - <a class="no-line" href="javascript:showCreateTable(\''
	 + database["databaseID"] + '\', \'' + database["name"] + '\')">									\
	<span class="create-button">create new table</span></a>									\
    - <a class="no-line" href="javascript:freezeDatabase(\''
	 + database["databaseID"] + '\')">																	\
	<span class="modify-button">freeze all tables</span></a>								\
    - <a class="no-line" href="javascript:unfreezeDatabase(\''
	 + database["databaseID"] + '\')">																	\
	<span class="modify-button">unfreeze all tables</span></a><br/><br/>								\
	';
	var div = document.createElement("div");
	div.setAttribute("class", "database");
	div.innerHTML = html;
	
	for (var i in database["tables"])
	{
		tableID = database["tables"][i];
		table = locateTable(configState, tableID);
		if (table == null)
			continue;
		var n = createTableDiv(configState, table);
		div.appendChild(n);
	}
	
	return div;
}

function createTableDiv(configState, table)
{
	var html =
	'																									\
		<table class="table">																			\
			<tr>																						\
				<td class="table-head">																	\
					<span class="table-head">' + table["name"] + '</span>								\
				</td>																					\
				<td>																					\
					Table number: ' + table["tableID"] + '<br/>											\
					Shards: 																			\
	';
	
	var quorumIDs = new Array();
	var rfactor = 0;
	var size = 0;
	table["shards"].sort();
	for (var i in table["shards"])
	{
		var shardID = table["shards"][i];
		var shard = locateShard(configState, shardID);
		if (shard == null)
			continue;
		size += shard["shardSize"];
		html += ' <span class="shard-number ' + scaliendb.getQuorumState(configState, shard["quorumID"]) + '">' + shardID + '</span> ';
		quorumID = shard["quorumID"];
		if (!contains(quorumIDs, quorumID))
			quorumIDs.push(quorumID);
		var quorum = locateQuorum(configState, quorumID);
		if (quorum == null)
			continue;
		if (quorum["activeNodes"].length > rfactor)
			rfactor = quorum["activeNodes"].length;
	}
	
	html +=
	'				<br/>																				\
					Mapped quorums: 																	\
	';
	quorumIDs.sort();
	for (var i in quorumIDs)
	{
			var quorumID = quorumIDs[i];
			html += '<span class="quorum-number ' + scaliendb.getQuorumState(configState, quorumID) + '">' + quorumID + '</span> ';
	}
	
	html += 
	'																									\
					<br/>																				\
					Replication factor: ' + rfactor + '<br/>											\
					Shard splitting frozen: ' + (table["isFrozen"] ? "yes" : "no") + '<br/>				\
					Size: ' + scaliendb.util.humanBytes(size) + '<br/><br/>												\
					<a class="no-line" href="javascript:showhideShardsDiv(\'' +
					table["tableID"] +  '\')">															\
					<span id="showhideShardsButton_' + table["tableID"] + '" class="modify-button">' + 
					(tableShardsVisible[tableID] ? 'hide shards' : 'show shards') + '</span></a>			\
				</td>																					\
				<td class="table-actions">																\
					<a class="no-line" href="javascript:showRenameTable(\'' +
					table["tableID"] + '\', \'' + table["name"] +  '\')">								\
					<span class="modify-button">rename table</span></a><br/><br/>						\
					<a class="no-line" href="javascript:showTruncateTable(\'' +
					table["tableID"] +  '\')">															\
					<span class="delete-button">truncate table</span></a><br/><br/>						\
					<a class="no-line" href="javascript:showDeleteTable(\'' +
					table["tableID"] +  '\')">															\
					<span class="delete-button">delete table</span>										\
					<a class="no-line" href="javascript:' + (table["isFrozen"] ? "unfreeze" : "freeze" ) + 'Table(\'' +
					table["tableID"] + '\')"><br/><br/>													\
					<span class="modify-button">' + (table["isFrozen"] ? "unfreeze" : "freeze" ) + ' table</span></a><br/><br/>						\
				</td>																					\
			</tr>																						\
		</table>																						\
	';
	var div = document.createElement("div");
	div.setAttribute("class", "table " + scaliendb.getTableState(configState, table["tableID"]));
	div.innerHTML = html;
	
	var shardsDiv = document.createElement("div");
	shardsDiv.setAttribute("id", "shards_" + table["tableID"]);
	if (tableShardsVisible[tableID])
		shardsDiv.setAttribute("style", "display:block");
	else
		shardsDiv.setAttribute("style", "display:none");
	
	for (var i in table["shards"])
	{
		shardID = table["shards"][i];
		shard = locateShard(configState, shardID);
		if (shard == null)
			continue;
		var shardDiv = createShardDiv(configState, shard);
		shardsDiv.appendChild(shardDiv);
	}
	
	div.appendChild(shardsDiv);
	
	return div;
}

function createShardDiv(configState, shard)
{
	var migrateShard = "";
	if (configState["quorums"].length > 1)
	{
		migrateShard = '<a class="no-line" href="javascript:showMigrateShard(\'' +
					shard["shardID"] +  '\')">															\
					<span class="modify-button">migrate shard</span></a><br/><br/>';
	}

	var html =
	'																									\
		<table class="shard">																			\
			<tr>																						\
				<td class="shard-head">																	\
					<span class="shard-head">Shard ' + shard["shardID"] + '</span>						\
				</td>																					\
				<td>																					\
	';

	
	html += 
	'																									\
					Quorum: <span class="quorum-number ' + scaliendb.getQuorumState(configState, shard["quorumID"]) + '">' + shard["quorumID"] + '</span> (' + scaliendb.getQuorumState(configState, shard["quorumID"]) + ')<br/>												\
					Start key: ' + (shard["firstKey"] == "" ? "(empty)" : shard["firstKey"]) + '<br/>							  		  		\
					End key: ' + (shard["lastKey"] == "" ? "(empty)" : shard["lastKey"]) + '<br/>												\
					Splitable: ' + (shard["isSplitable"] ? "yes" : "no") + '<br/>';
					if (shard["isSplitable"])
						html +=  'Split key: ' + (scaliendb.util.defstr(shard["splitKey"]) == "" ? "(empty)" : scaliendb.util.defstr(shard["splitKey"])) + '<br/>';
					html += '																			\
					Size: ' + scaliendb.util.humanBytes(shard["shardSize"]) + '<br/>	  				\
				</td>																					\
				<td class="shard-actions">																\
					<a class="no-line" href="javascript:showSplitShard(\'' +
					shard["shardID"] + '\')">								\
					<span class="modify-button">split shard</span></a><br/><br/>' + 
					migrateShard +					
				'</td>																					\
			</tr>																						\
		</table>																						\
	';
	
	var div = document.createElement("div");
	div.setAttribute("class", "shard " + scaliendb.getQuorumState(configState, shard["quorumID"]));
	div.innerHTML = html;
	return div;
}

function createMigrationDivs(configState, shardServers)
{
	scaliendb.util.removeElement("migrations");
	var migrationsDiv = document.createElement("div");
	migrationsDiv.setAttribute("id", "migrations");
	
	var count = 0;
	for (var i in shardServers)
	{
		var shardServer = shardServers[i];
		for (var j in shardServer["quorumShardInfos"])
		{
			var quorumShardInfo = shardServer["quorumShardInfos"][j];
			if (quorumShardInfo["isSendingShard"])
			{
				migrationsDiv.appendChild(createMigrationDiv(configState, shardServer, quorumShardInfo));
				count++;
			}
		}
	}
	
	$("tabPageMigration").appendChild(migrationsDiv);
	$("tabHeadMigration").innerHTML = "Shard migration";
	if (count > 0)
		$("tabHeadMigration").innerHTML += " (" + count + ")";
}

function createMigrationDiv(configState, shardServer, quorumShardInfo)
{
	var shard = locateShard(configState, quorumShardInfo["shardID"]);
	var html =
	'																									\
	<table class="migration healthy">																	\
		<tr>																							\
			<td class="migration-head">																	\
				<span class="migration-head">shard ' + quorumShardInfo["shardID"] + ' </span>			\
			</td>																						\
			<td>																						\
				Shard servers: <span class="shardserver-number healthy">' +  shardServer["nodeID"] + '</span> ' + ' &rarr; <span class="shardserver-number healthy">' + quorumShardInfo["migrationNodeID"] +  '</span><br/>								\
				Quorum: <span class="quorum-number healthy">' + shard["quorumID"] + '</span> &rarr; <span class="quorum-number healthy">' + quorumShardInfo["migrationQuorumID"] + '</span><br/>						\
				Progress: ' + scaliendb.util.humanBytes(quorumShardInfo["migrationBytesSent"]) + '/' + scaliendb.util.humanBytes(quorumShardInfo["migrationBytesTotal"]) + ' (' + scaliendb.util.humanBytes(quorumShardInfo["migrationThroughput"]) + '/s)	\
			</td>																						\
		</tr>																							\
	</table>																							\
	';

	var div = document.createElement("div");
	div.setAttribute("class", "migration healthy");
	div.innerHTML = html;
	return div;
}

function locateTable(configState, tableID)
{
	for (var key in configState)
	{
		if (key == "tables")
		{
			tables = configState[key];
			for (var table in tables)
			{
				tbl = tables[table];
				if (tbl["tableID"] == tableID)
					return tbl;
			}
		}
	}
	
	return null;
}

function locateShard(configState, shardID)
{
	for (var key in configState)
	{
		if (key == "shards")
		{
			shards = configState[key];
			for (var shard in shards)
			{
				shd = shards[shard];
				if (shd["shardID"] == shardID)
					return shd;
			}
		}
	}
	
	return null;
}

function locateQuorum(configState, quorumID)
{
	for (var key in configState)
	{
		if (key == "quorums")
		{
			quorums = configState[key];
			for (var quorum in quorums)
			{
				q = quorums[quorum];
				if (q["quorumID"] == quorumID)
					return q;
			}
		}
	}
	
	return null;
}

function consoleEvalExpression()
{
	var consoleForm = $("console-form");
	var cmd = consoleForm.input.value;
	consoleForm.output.value += "Executing " + cmd + "\n";
	eval(cmd);
}

function consoleOnKeyDown(e)
{
	if ((window.event && window.event.keyCode == 13) ||
	    (e && e.keyCode == 13))
	{
		alert("Enter pressed");
	}
}

function removeOutline()
{
	var links = document.getElementsByTagName("a");
 	for( i=0; i < links.length; i++)
 	{
 		links[i].onfocus = links[i].blur;
 	}
}

function getKeycode(e)
{
	if (e == null)
		return event.keyCode;
	else
		return e.which;
}
