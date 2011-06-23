var scaliendb = 
{
	controller: "",
	onResponse: this.showResult,
	onDisconnect: this.showError,

    //========================================================================
	//
	// ScalienDB Client interface
	//
	//========================================================================
	
	getQuorumState: function(configState, quorumID)
	{
		var quorum = locateQuorum(configState, quorumID);
		if (quorum["hasPrimary"] == "true")
		{
			if (quorum["inactiveNodes"].length == 0)
				state = "healthy";
			else
				state = "unhealthy";
		}
		else
			state = "critical";
	
		return state;
	},
	
	getQuorumInfo: function(configState, nodeID, quorumID)
	{
		var shardServer = scaliendb.getShardServer(configState, nodeID);
		for (var i in shardServer["quorumInfos"])
		{
			var quorumInfo = shardServer["quorumInfos"][i];
			if (quorumInfo["quorumID"] == quorumID)
				return quorumInfo;
		}
		return null;
	},

	getTableState: function(configState, tableID)
	{
		var state = "healthy";
		var table = scaliendb.getTable(configState, tableID);
		for (var i in table["shards"])
		{
			var shardID = table["shards"][i];
			var shard = locateShard(configState, shardID);
			var quorumState = scaliendb.getQuorumState(configState, shard["quorumID"]);
			if (state == "critical")
				continue;
			if (state == "unhealthy" && quorumState == "critical")
				state = quorumState;
			if (state == "healthy" && quorumState != "healthy")
				state = quorumState;
		}	
		return state;
	},

	getClusterState: function(configState)
	{
		var state = "healthy";
		for (var i in configState["quorums"])
		{
			var quorum = configState["quorums"][i];
			var quorumState = scaliendb.getQuorumState(configState, quorum["quorumID"]);
			if (state == "critical")
				continue;
			if (state == "unhealthy" && quorumState == "critical")
				state = quorumState;
			if (state == "healthy" && quorumState != "healthy")
				state = quorumState;
		}
		return state;
	},

	findMaster: function(onFindMaster)
	{ 
		this.json.rpc(scaliendb.controller, onFindMaster, scaliendb.onDisconnect, "getmasterhttp");
	},
	
	getConfigState: function(onConfigState)
	{ 
		this.json.rpc(scaliendb.controller, onConfigState, scaliendb.onDisconnect, "getconfigstate");
	},

	pollConfigState: function(onConfigState, paxosID, changeTimeout)
	{                                                                                                       
		var params = {};
		params["paxosID"] = paxosID;
		params["changeTimeout"] = changeTimeout;
		this.json.rpc(scaliendb.controller, onConfigState, scaliendb.onDisconnect, "pollconfigstate", params);
	},

	getTable: function(configState, tableID)
	{
		for (var i in configState["tables"])
		{
			var table = configState["tables"][i];
			if (table["tableID"] == tableID)
				return table;
		}
		
		return null;
	},
	
	getShardServer: function(configState, nodeID)
	{
		for (var i in configState["shardServers"])
		{
			var shardServer = configState["shardServers"][i];
			if (shardServer["nodeID"] == nodeID)
				return shardServer;
		}
		
		return null;
	},
	
	createQuorum: function(name, nodes)
	{                                                                                 
		var params = {};
		params["name"] = name;
		params["nodes"] = nodes;
		this.rpc("createquorum", params);
	},

	renameQuorum: function(quorumID, name)
	{                                                                                 
		var params = {};
		params["quorumID"] = quorumID;
		params["name"] = name;
		this.rpc("renamequorum", params);
	},

	deleteQuorum: function(quorumID)
	{                                                                                 
		var params = {};
		params["quorumID"] = quorumID;
		this.rpc("deletequorum", params);
	},

	addNode: function(quorumID, nodeID)
	{                                                                                 
		var params = {};
		params["quorumID"] = quorumID;
		params["nodeID"] = nodeID;
		this.rpc("addnode", params);
	},

	removeNode: function(quorumID, nodeID)
	{                                                                                 
		var params = {};
		params["quorumID"] = quorumID;
		params["nodeID"] = nodeID;
		this.rpc("removenode", params);
	},

	activateNode: function(quorumID, nodeID)
	{                                                                                 
		var params = {};
		params["quorumID"] = quorumID;
		params["nodeID"] = nodeID;
		this.rpc("activatenode", params);
	},

	createDatabase: function(name)
	{ 
		var params = {};
		params["name"] = name;
		this.rpc("createdatabase", params);
	},

	deleteDatabase: function(name)
	{ 
		var params = {};
		params["name"] = name;
		this.rpc("deletedatabase", params);
	},
	
	renameDatabase: function(databaseID, name)
	{ 
		var params = {};
		params["databaseID"] = databaseID;
		params["name"] = name;
		this.rpc("renamedatabase", params);
	},
	
	deleteDatabase: function(databaseID)
	{ 
		var params = {};
		params["databaseID"] = databaseID;
		this.rpc("deletedatabase", params);
	},
		
	createTable: function(databaseID, quorumID, name)
	{
		var params = {};
		params["databaseID"] = databaseID;
		params["quorumID"] = quorumID;
		params["name"] = name;
		this.rpc("createtable", params);
	},

	renameTable: function(tableID, name)
	{
		var params = {};
		params["tableID"] = tableID;
		params["name"] = name;
		this.rpc("renametable", params);
	},

	deleteTable: function(tableID)
	{
		var params = {};
		params["tableID"] = tableID;
		this.rpc("deletetable", params);
	},

	truncateTable: function(tableID)
	{
		var params = {};
		params["tableID"] = tableID;
		this.rpc("truncatetable", params);
	},

	freezeTable: function(tableID)
	{
		var params = {};
		params["tableID"] = tableID;
		this.rpc("freezetable", params);
	},

	unfreezeTable: function(tableID)
	{
		var params = {};
		params["tableID"] = tableID;
		this.rpc("unfreezetable", params);
	},

	splitShard: function(shardID, key)
	{
		var params = {};
		params["shardID"] = shardID;
		params["key"] = key;
		this.rpc("splitshard", params);
	},

	migrateShard: function(shardID, quorumID)
	{
		var params = {};
		params["shardID"] = shardID;
		params["quorumID"] = quorumID;
		this.rpc("migrateshard", params);
	},
	
	showResult: function(data)
	{
		//alert(data["response"]);
	},
	
	showError: function()
	{
		alert("connection lost");
	},
	
	rpc: function(name, params)
	{ 
		this.json.rpc(scaliendb.controller, scaliendb.onResponse, scaliendb.onDisconnect, name, params);
	},
	
	disconnect: function()
	{
		this.json.abort();
	},
		
    //========================================================================
	//
	// JSON utilities
	//
	//========================================================================
	json:
	{
		counter : 0,
		active : 0,
		func: {},
		debug: true,
		pollRequest: null,

		getJSONP: function(url, userfunc, errorfunc, showload)
		{
			var id = this.counter++;

			url += "&callback=scaliendb.json.func[" + id + "]";
			var scriptTag = document.createElement("script");
			scriptTag.setAttribute("id", "json" + id);
			scriptTag.setAttribute("type", "text/javascript");
			scriptTag.setAttribute("src", url);
			document.getElementsByTagName("head")[0].appendChild(scriptTag);
			if (showload)
				this.active++;
			scaliendb.util.trace("[" + this.active + "] calling " + url);

			this.func[id] = function(data)
			{
				if (data == undefined)
				{
					if (this.debug)
						alert("json.func[" + id + "]: empty callback");
					scaliendb.util.trace("json.func[" + id + "]: empty callback");
					errorfunc();
					return;
				}

				if (data.hasOwnProperty("error"))
				{
					if (this.debug)
						alert("json.func[" + id + "]: " + data.error);
					scaliendb.util.trace("json.func" + id + ": " + data.error);

					if (data.hasOwnProperty("type") && data.type == "session")
					{
						scaliendb.util.logout();
						return;
					}
				}

				scaliendb.util.trace("[" + this.active + "] json callback " + id);			
				userfunc(data);
				if (showload)
					this.active--;
				scaliendb.util.trace("[" + this.active + "] json callback " + id + " after userfunc");
				scaliendb.util.removeElement("json" + id);
				this.func = scaliendb.util.removeKey(this.func, "func" + id);
			}
		},
		
		getXHR: function(url, userfunc, errorfunc, showload)
		{
			var xhr = new XMLHttpRequest();
			var decode = this.decode;
			var onreadystatechange = function()
			{
				if (xhr.readyState != 4)
					return;
				if (xhr.status != 200)
				{   
					// pollRequest == null indicates that the request is aborted
					if (scaliendb.json.pollRequest !== null)
						errorfunc();
					scaliendb.json.pollRequest = null;
					return;
				}
				scaliendb.json.pollRequest = null;
				userfunc(decode(xhr.responseText));
			}

			xhr.open("GET", url + "&origin=*", true);
			xhr.onreadystatechange = onreadystatechange;
			this.pollRequest = xhr;
			xhr.send();
		},

		rpc: function(baseUrl, userfunc, errorfunc, cmd, params)
		{
			var url = baseUrl + cmd + "?mimetype=text/javascript";
			for (var name in params)
			{
				url += "&" + name + "=";
				param = params[name];
				if (typeof(param) != "undefined")
				{
					if (typeof(param) == "object" || typeof(param) == "array")
						var arg = this.encode(param);
					else
						var arg = param;
					var value = encodeURIComponent(arg);
				}
				else
					var value = "";
				url += value;
			}
			// TODO: detecting browser functionality
			// this.getJSONP(url, userfunc, errorfunc, true);   
			this.abort();
			this.getXHR(url, userfunc, errorfunc, true);
		},
	
		abort: function()
		{
			if (this.pollRequest != null)
			{
				var request = this.pollRequest;
				// this indicates that the request is aborted
				this.pollRequest = null;
				request.abort();
			}
		},
	
		encode: function(jsobj)
		{
			// if (typeof(jsobj) == "undefined")
			//  	return "";
		
			return JSON.stringify(jsobj);
		},
		
		decode: function(jsontext)
		{
			return JSON.parse(jsontext);
		}
	},

    //========================================================================
	//
	// Other utility functions
	//
	//========================================================================	
	util:
	{
	 	elem: function(id)
		{
			return document.getElementById(id);
		},

		keys: function(arr)
		{
			var r = new Array();
			for (var key in arr)
				r.push(key);
			return r;
		},
	
	   removeKey: function(arr, key)
		{
			var n = new Array();
			for (var i in arr)
				if (i != key)
					n.push(arr[i]);	
			return n;
		},
	
		removeElement: function(id)
		{
			e = this.elem(id);
			e.parentNode.removeChild(e);
		},
	
		padString: function(str, width, pad)
		{
			str = "" + str;
			while (str.length < width)
				str = pad + str;
	
			return str;
		},

		escapeQuotes: function(str)
		{
			str = str.replace(/'/g, "\\x27");
			str = str.replace(/\"/g, "\\x22");
			return str;
		},
	
		trace: function(msg)
		{
		},
		
		startsWith: function(str, s)
		{
			return (str.indexOf(s) == 0)
		},
		
		endsWith: function(str, s)
		{
			return (str.indexOf(s) == (str.length - s.length));
		},
		
		removeSpaces: function(str)
		{
			return str.replace(/ /g,'')
		},
	
		clear: function(node)
		{
			while (node.childNodes.length > 0)
			{
			    node.removeChild(node.firstChild);
			}
		},
		
		defstr: function(val, defval)
		{
			if (typeof val == 'undefined')
			{
				if (typeof defval == 'undefined')
					return "";
				return "" + defval;
			}
			return "" + val;
		},
		
		numDigits: function(num)
		{
			return ("" + num).length;
		},
		
		humanBytes: function(size)
		{
			var units = "kMGTPEZY";
			var n = size;
			var m = size;
			var u = 0;
			
			if (typeof size == 'undefined')
				return "0";
			
			while (this.numDigits(n) > 3)
			{
				m = Math.round(n / 100.0) / 10.0;
				n = Math.round(n / 1000.0);
				u++;
			}
			
			var dim = (u == 0 ? "" : units[u - 1]);
			return "" + m + dim;
		},
		
		pluralize: function(count, noun)
		{
			// add exceptions here
			if (count <= 1)
				return noun;
			else
				return noun + "s";
		},
		
		cardinality: function(count, noun)
		{
			return count + " " + scaliendb.util.pluralize(count, noun);
		}
	}	
}


