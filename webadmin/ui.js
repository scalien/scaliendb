function onLoad()
{
	scaliendb.util.elem("loginContainer").style.display = "block";
	scaliendb.util.elem("mainContainer").style.display = "none";
	scaliendb.util.elem("createQuorumContainer").style.display = "none";
	scaliendb.util.elem("deleteQuorumContainer").style.display = "none";
	scaliendb.util.elem("addNodeContainer").style.display = "none";
	scaliendb.util.elem("removeNodeContainer").style.display = "none";
	scaliendb.util.elem("createDatabaseContainer").style.display = "none";
	scaliendb.util.elem("createTableContainer").style.display = "none";
	scaliendb.util.elem("loginCluster").focus();
	removeOutline();
}

function logout()
{
	clearTimeout(timer);
	onLoad();
}

function connect()
{
	scaliendb.util.elem("loginContainer").style.display = "none";
	scaliendb.util.elem("mainContainer").style.display = "block";
	
	var controller = scaliendb.util.elem("loginCluster").value;
	if (!scaliendb.util.startsWith(controller, "http://"))
		controller = "http://" + controller;
	if (!scaliendb.util.endsWith("controller", "/json/"))
		controller = controller + "/json/";
	if (controller !== scaliendb.controller)
		scaliendb.controller = controller;
	updateConfigState();
}

tabs = ["Dashboard", "Quorums", "Schema", "Shards", "Console"];

function activateTab(name)
{
	for (var t in tabs)
	{
		var tab = tabs[t];
		if (tab == name)
		{
			scaliendb.util.elem("tabHead" + tab).className = "tab-head tab-head-active";
			scaliendb.util.elem("tabPage" + tab).className = "tab-page tab-page-active";
		}
		else
		{
			scaliendb.util.elem("tabHead" + tab).className = "tab-head tab-head-inactive";
			scaliendb.util.elem("tabPage" + tab).className = "tab-page tab-page-inactive";			
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

function activateShardsTab()
{
	activateTab("Shards");
}

function activateConsoleTab()
{
	activateTab("Console");
}

function showCreateQuorum()
{
	scaliendb.util.elem("createQuorumContainer").style.display = "block";
	scaliendb.util.elem("createQuorumShardServers").focus();
}

var deleteQuorumID; // TODO: hack global
function showDeleteQuorum(quorumID)
{
	scaliendb.util.elem("deleteQuorumContainer").style.display = "block";
	deleteQuorumID = quorumID;
}

var addNodeQuorumID; // TODO: hack global
function showAddNode(quorumID)
{
	scaliendb.util.elem("addNodeContainer").style.display = "block";
	addNodeQuorumID = quorumID;
}

var removeNodeQuorumID; // TODO: hack global
function showRemoveNode(quorumID)
{
	scaliendb.util.elem("removeNodeContainer").style.display = "block";
	removeNodeQuorumID = quorumID;
}

function showCreateDatabase()
{
	scaliendb.util.elem("createDatabaseContainer").style.display = "block";
	scaliendb.util.elem("createDatabaseName").focus();
}

var createTableDatabaseID; // TODO: hack global
function showCreateTable(databaseID, databaseName)
{
	scaliendb.util.elem("createTableDatabase").textContent = databaseName;
	scaliendb.util.elem("createTableContainer").style.display = "block";
	scaliendb.util.elem("createTableName").focus();
	createTableDatabaseID = databaseID;
}

function hideCreateQuorum()
{
	scaliendb.util.elem("createQuorumContainer").style.display = "none";
	scaliendb.util.elem("mainContainer").style.display = "block";
}

function hideDeleteQuorum()
{
	scaliendb.util.elem("deleteQuorumContainer").style.display = "none";
	scaliendb.util.elem("mainContainer").style.display = "block";
}

function hideAddNode()
{
	scaliendb.util.elem("addNodeContainer").style.display = "none";
	scaliendb.util.elem("mainContainer").style.display = "block";
}

function hideRemoveNode()
{
	scaliendb.util.elem("removeNodeContainer").style.display = "none";
	scaliendb.util.elem("mainContainer").style.display = "block";
}

function hideCreateDatabase()
{
	scaliendb.util.elem("createDatabaseContainer").style.display = "none";
	scaliendb.util.elem("mainContainer").style.display = "block";
}

function hideCreateTable()
{
	scaliendb.util.elem("createTableContainer").style.display = "none";
	scaliendb.util.elem("mainContainer").style.display = "block";
}

function createQuorum()
{
	hideCreateQuorum();
	
	var nodes = scaliendb.util.elem("createQuorumShardServers").value;
	nodes = scaliendb.util.removeSpaces(nodes);
	scaliendb.onResponse = onResponse;
	scaliendb.createQuorum(nodes);
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

	var nodeID = scaliendb.util.elem("addNodeShardServer").value;
	nodeID = scaliendb.util.removeSpaces(nodeID);
	scaliendb.onResponse = onResponse;
	scaliendb.addNode(addNodeQuorumID, nodeID);
}

function removeNode()
{
	hideRemoveNode();

	var nodeID = scaliendb.util.elem("removeNodeShardServer").value;
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
	var name = scaliendb.util.elem("createDatabaseName").value;
	name = scaliendb.util.removeSpaces(name);
	scaliendb.onResponse = onResponse;
	scaliendb.createDatabase(name);
}

function createTable()
{
	hideCreateTable();
	var name = scaliendb.util.elem("createTableName").value;
	name = scaliendb.util.removeSpaces(name);
	var quorumID = scaliendb.util.elem("createTableQuorum").value;
	quorumID = scaliendb.util.removeSpaces(quorumID);
	scaliendb.onResponse = onResponse;
	scaliendb.createTable(createTableDatabaseID, quorumID, name);
}

function onResponse()
{
	updateConfigState();
}

function updateConfigState()
{
	scaliendb.getConfigState(onConfigState);
}

var timer;
function onConfigState(configState)
{
	scaliendb.util.elem("controller").textContent = scaliendb.controller;

	createDashboard(configState);
	
	for (var key in configState)
	{
		if (key == "quorums")
			createQuorumDivs(configState, configState[key]);
		else if (key == "databases")
			createDatabaseDivs(configState, configState[key]);
		else if (key == "shards")
			createShardDivs(configState, configState[key]);
	}
	
	clearTimeout(timer);
	timer = setTimeout("onTimeout()", 1000);
}

function onTimeout()
{
	updateConfigState();
}

function createDashboard(configState)
{
	var numDatabases = 0;
	var numTables = 0;
	var maxTables = 0;
	var minTables = 999999999;
	var avgTables = 0;
	var numShards = 0;
	var maxShards = 0;
	var minShards = 999999999;
	var avgShards = 0;
	var shardServerIDs = new Array();
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
		else if (key == "shardServers")
		{
			var shardServers = configState[key]; 
			numShardServers = shardServers.length;
			for (var shardServer in shardServers)
			{
				var ss = shardServers[shardServer];
				shardServerIDs.push(ss["nodeID"]);
			}
		}
	}
	avgTables = Math.round(numTables / numDatabases * 10) / 10;
	avgShards = Math.round(numShards/ numTables * 10) / 10;
	
	scaliendb.util.elem("numDatabases").textContent = numDatabases;
	scaliendb.util.elem("numTables").textContent = numTables;
	scaliendb.util.elem("numShards").textContent = numShards;
	scaliendb.util.elem("numQuorums").textContent = numQuorums;
	scaliendb.util.elem("numShardServers").textContent = numShardServers;
	

	scaliendb.util.elem("minTables").textContent = minTables;
	scaliendb.util.elem("maxTables").textContent = maxTables;
	scaliendb.util.elem("avgTables").textContent = avgTables;

	scaliendb.util.elem("minShards").textContent = minShards;
	scaliendb.util.elem("maxShards").textContent = maxShards;
	scaliendb.util.elem("avgShards").textContent = avgShards;
	
	if (numDatabases == 0 || numTables == 0)
		scaliendb.util.elem("dashboardStats").style.display = "none";
	else
		scaliendb.util.elem("dashboardStats").style.display = "inline";
	
	scaliendb.util.clear(scaliendb.util.elem("shardservers"));
	
	var html = '';
	for (var i in shardServerIDs)
	{
		nodeID = shardServerIDs[i];
		html += ' <span class="healthy shardserver-number">' + nodeID + '</span> ';
	}
	scaliendb.util.elem("shardservers").innerHTML = html;
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
	
	scaliendb.util.elem("tabPageQuorums").appendChild(quorumsDiv);
}

function createQuorumDiv(configState, quorum)
{
	var state;
	var primaryID = null;
	if (quorum["hasPrimary"] == "true")
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
				<span class="quorum-head">quorum ' + quorum["quorumID"] + '</span>						\
			</td>																						\
			<td>																						\
				Shardservers: 																			\
	';
	for (var i in quorum["activeNodes"])
	{
		nodeID = quorum["activeNodes"][i];
		if (nodeID == primaryID)
		 	html += ' <span class="primary shardserver-number">' + nodeID + '</span> ';
		else
		 	html += ' <span class="secondary shardserver-number">' + nodeID + '</span> ';
	}
	for (var i in quorum["inactiveNodes"])
	{
		nodeID = quorum["inactiveNodes"][i];
		html += ' <a class="no-line" title="Activate shard server" href="javascript:activateNode(' + quorum["quorumID"] + ", " + nodeID + ')"><span class="inactive shardserver-number">' + nodeID + '</span></a> ';
	}
	html +=
	'			<br/>																					\
				Shards: 																				\
	';
	for (var shardID in quorum["shards"])
	{
		html += ' <span class="shard-number">' + shardID + '</span> ';
	}
	html +=
	'																									\
				<br/>																					\
				Replication round: ' + paxosID + '<br/>													\
				<!-- Size: 0GB -->																		\
			</td>																						\
			<td class="table-actions">																	\
				<a class="no-line" href="javascript:showAddNode(' + quorum["quorumID"] +
				')"><span class="create-button">add server</span></a><br/><br/>									\
				<a class="no-line" href="javascript:showRemoveNode(' + quorum["quorumID"] +
				')"><span class="modify-button">remove server</span></a><br/><br/>									\
				<a class="no-line" href="javascript:showDeleteQuorum(' + quorum["quorumID"] +
				')"><span class="delete-button">delete quorum</span></a>										\
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
	
	scaliendb.util.elem("tabPageSchema").appendChild(databasesDiv);
}

function createDatabaseDiv(configState, database)
{
	var html =
	'																									\
	<span class="database-head">Listing tables for database	 <b>' + database["name"] + '</b></span>		\
	 - <a class="no-line" href="javascript:showCreateTable(\''
	 + database["databaseID"] + '\', \'' + database["name"] + '\')">																								\
	<span class="create-button">create new table</span></a><br/><br/>									\
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
					Shards: 																			\
	';
	
	var quorumIDs = new Array();
	var rfactor = 0;
	for (var i in table["shards"])
	{
		var shardID = table["shards"][i];
		var shard = locateShard(configState, shardID);
		if (shard == null)
			continue;
		html += ' <span class="shard-number">' + shardID + '</span> ';
		quorumID = shard["quorumID"];
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
	
	for (var i in quorumIDs)
	{
			var quorumID = quorumIDs[i];
			html += ' <span class="quorum-number">' + quorumID + '</span> ';
	}
	
	html += 
	'																									\
					<br/>															\
					Replication factor: ' + rfactor + '<br/>											\
				</td>																					\
				<td class="table-actions">																\
					<span class="modify-button">rename table</span><br/><br/>							\
					<span class="delete-button">delete table</span>										\
				</td>																					\
			</tr>																						\
		</table>																						\
	';
	
	var div = document.createElement("div");
	div.setAttribute("class", "table healthy");
	div.innerHTML = html;
	return div;
}

function createShardDivs(configState, shards)
{
	scaliendb.util.removeElement("shards");
	var shardsDiv = document.createElement("div");
	shardsDiv.setAttribute("id", "shards");
	
	for (var shard in shards)
	{
		var s = shards[shard];
		var shardDiv = createShardDiv(configState, s);
		shardsDiv.appendChild(shardDiv);
	}
	
	scaliendb.util.elem("tabPageShards").appendChild(shardsDiv);
}

function createShardDiv(configState, shard)
{
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
					QuorumID: ' + shard["quorumID"] + '<br/>											\
					DatabaseID: ' + shard["databaseID"] + '<br/>  										\
					TableID: ' + shard["tableID"] + '<br/>												\
					Start: "' + shard["firstKey"] + '"<br/>							  		  			\
					End: "' + shard["lastKey"] + '"<br/>												\
					Split key: "' + scaliendb.util.defstr(shard["splitKey"]) + '"<br/> 					\
					Size: ' + scaliendb.util.defstr(shard["shardSize"], 0) + '<br/>	  					\
				</td>																					\
				<td class="shard-actions">																\
					<span class="modify-button">split shard</span><br/><br/>							\
				</td>																					\
			</tr>																						\
		</table>																						\
	';
	
	var div = document.createElement("div");
	div.setAttribute("class", "shard healthy");
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
	var consoleForm = scaliendb.util.elem("console-form");
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
