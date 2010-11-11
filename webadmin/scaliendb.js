var scaliendb = 
{
	controller: "http://localhost:8080/json/",
	onResponse: this.showResult,

    //========================================================================
	//
	// ScalienDB Client interface
	//
	//========================================================================
	getConfigState: function(onConfigState)
	{ 
		this.json.rpc(scaliendb.controller, onConfigState, "getconfigstate");
	},

	createDatabase: function(name)
	{ 
		var params = {};
		params["name"] = name;
		this.json.rpc(scaliendb.controller, scaliendb.onResponse, "createdatabase", params);
	},
	
	createQuorum: function(nodes)
	{                                                                                 
		var params = {};
		params["nodes"] = nodes;
		this.json.rpc(scaliendb.controller, scaliendb.onResponse, "createquorum", params);
	},
	
	createTable: function(databaseID, quorumID, name)
	{
		var params = {};
		params["databaseID"] = databaseID;
		params["quorumID"] = quorumID;
		params["name"] = name;
		this.json.rpc(scaliendb.controller, scaliendb.onResponse, "createtable", params);
	},
	
	showResult: function(data)
	{
		//alert(data["response"]);
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

		get: function(url, userfunc, showload)
		{
			var id = this.counter++;

			url = url.replace(/USERFUNC/g, "scaliendb.json.func[" + id + "]");
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
			this.get(url, userfunc, true);
		},
	
		encode: function(jsobj)
		{
			// if (typeof(jsobj) == "undefined")
			//  	return "";
		
			return JSON.stringify(jsobj);
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
		}
	}	
}


