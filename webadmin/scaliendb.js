var scaliendb = 
{
	controller: "http://localhost:8080/json/",
	onResponse: this.showResult,
    
	getConfigState: function(onConfigState)
	{ 
		json.rpc(scaliendb.controller, onConfigState, "getconfigstate");
	},

	createDatabase: function(name)
	{ 
		var params = {};
		params["name"] = name;
		json.rpc(scaliendb.controller, scaliendb.onResponse, "createdatabase", params);
	},
	
	createQuorum: function(nodes)
	{                                                                                 
		var params = {};
		params["nodes"] = nodes;
		json.rpc(scaliendb.controller, scaliendb.onResponse, "createquorum", params);
	},
	
	createTable: function(databaseID, quorumID, name)
	{
		var params = {};
		params["databaseID"] = databaseID;
		params["quorumID"] = quorumID;
		params["name"] = name;
		json.rpc(scaliendb.controller, scaliendb.onResponse, "createtable", params);
	},
	
	showResult: function(data)
	{
		//alert(data["response"]);
	},
		
	lastJSONmember: null
}

var json =
{
	counter : 0,
	active : 0,
	func: {},
	debug: true,

	get: function(url, userfunc, showload)
	{
		var id = json.counter++;

		url = url.replace(/USERFUNC/g, "json.func[" + id + "]");
		var scriptTag = document.createElement("script");
		scriptTag.setAttribute("id", "json" + id);
		scriptTag.setAttribute("type", "text/javascript");
		scriptTag.setAttribute("src", url);
		document.getElementsByTagName("head")[0].appendChild(scriptTag);
		if (showload)
			this.active++;
		util.trace("[" + json.active + "] calling " + url);

		this.func[id] = function(data)
		{
			if (data == undefined)
			{
				if (json.debug)
					alert("json.func[" + id + "]: empty callback");
				util.trace("json.func[" + id + "]: empty callback");
				return;
			}

			if (data.hasOwnProperty("error"))
			{
				if (json.debug)
					alert("json.func[" + id + "]: " + data.error);
				util.trace("json.func" + id + ": " + data.error);

				if (data.hasOwnProperty("type") && data.type == "session")
				{
					util.logout();
					return;
				}
			}

			util.trace("[" + json.active + "] json callback " + id);			
			userfunc(data);
			if (showload)
				json.active--;
			util.trace("[" + json.active + "] json callback " + id + " after userfunc");
			util.removeElement("json" + id);
			this.func = util.removeKey(this.func, "func" + id);
		}
	},

	rpc: function(baseUrl, userfunc, cmd, params)
	{
		var url = baseUrl + cmd + "?callback=USERFUNC";
		for (var name in params)
		{
			url += "&" + name + "=";
			param = params[name];
			if (typeof(param) != "undefined")
			{
				if (typeof(param) == "object" || typeof(param) == "array")
					var arg = JSON.stringify(param);
				else
					var arg = param;
				var value = encodeURIComponent(arg);
			}
			else
				var value = "";
			url += value;
		}                                        
		json.get(url, userfunc, true);
	},
	
	encode: function(jsobj)
	{
		// if (typeof(jsobj) == "undefined")
		//  	return "";
		
		return JSON.stringify(jsobj);
	}
}

var util =
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
	}	
}