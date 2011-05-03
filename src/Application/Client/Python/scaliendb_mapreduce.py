import scaliendb
import json
import time
from scaliendb_client import *

class MapReduceBase:
    def __init__(self, client):
        self.client = client
        self.result_table_id = client.get_table_id(client.get_current_database_id(), "result")
        self.reduce_table_id = client.get_table_id(client.get_current_database_id(), "reduce")

    def emit_intermediate(k, v):
        table_id = SDBP_GetCurrentTableID(self.client.cptr)
        SDBP_UseTableID(self.client.cptr, self.reduce_table_id)
        self.client.set(k, v)
        SDBP_UseTableID(self.client.cptr, self.table_id)

    def emit(k, v):
        table_id = SDBP_GetCurrentTableID(self.client.cptr)
        SDBP_UseTableID(self.client.cptr, self.result_table_id)
        self.client.set(k, v)
        SDBP_UseTableID(self.client.cptr, self.table_id)

    def clear(self):
        pass
        
    def map(self, k, v):
        pass
    
    def reduce(self, ):
        pass

class FindStartsWith(MapReduceBase):
    def __init__(self, client, starts_with):
        MapReduce.__init__(self, client)
        self.results = []
        self.starts_with = starts_with
        self.map_count = 0
    
	def clear(self):
		self.results = []
		
    def map(self, k, v):
        if v.find(self.starts_with) == 0:
            self.results.append((k, v))
            self.map_count += 1
    
    def reduce(self):
        for result in self.results:
            k, v = result
            print("key: " + k + ", value: " + v[:32])

class Contains(MapReduceBase):
    def __init__(self, client, word):
        MapReduce.__init__(self, client)
        self.results = []
        self.word = word
        self.map_count = 0
    
	def clear(self):
		self.results = []
		
    def map(self, k, v):
        if v.find(self.word) != -1:
            self.results.append((k, v))
            self.emit(k, v)
            self.map_count += 1
    
    def reduce(self):
        for result in self.results:
            k, v = result
            print("key: " + k + ", value: " + v[:32])


def map_reduce(self, mapred):
    mapred.clear()
    key = ""
    count = 1000
    offset = 0
    while True:
        status = SDBP_ListKeyValues(self.cptr, key, count, offset)
        self.result = scaliendb.Client.Result(SDBP_GetResult(self.cptr))
        if status < 0:
            return
        num, last_key = self.result.map(mapred.map)
        if num < count:
            break
        key = last_key
        offset = 1
    return mapred.reduce()

def map_shard(self, first_key, last_key, mapred):
    key = first_key
    count = 1000
    offset = 0
    while True:
        status = SDBP_ListKeyValues(self.cptr, key, last_key, count, offset)
        self.result = scaliendb.Client.Result(SDBP_GetResult(self.cptr))
        if status < 0:
            return
        num, last_key = self.result.map(mapred.map)
        if num < count:
            break
        key = last_key
        offset = 1

def local_map_reduce(client, endpoint, mapred):
    mapred.clear()
    table_id = client.get_current_table_id()
    shards = local_shards(client, endpoint, table_id)
    for shard in shards:
        map_shard(client, str(shard["firstKey"]), str(shard["lastKey"]), mapred)
    return mapred.reduce()

def local_shards(client, endpoint, table_id):
    config_state = json.loads(client.get_json_config_state())
    node_id = None
    for shard_server in config_state["shardServers"]:
        if shard_server["endpoint"] == endpoint:
            node_id = shard_server["nodeID"]
            break
    if node_id == None:
        return
    quorums = []
    for quorum in config_state["quorums"]:
        if quorum.get("primaryID", None) == node_id:
            quorums.append(quorum["quorumID"])
    if len(quorums) == 0:
        return
    shards = []
    for shard in config_state["shards"]:
        if shard["tableID"] == table_id and shard["quorumID"] in quorums:
            shards.append(shard)
    return shards

def local_quorums(config_state, endpoint):
    quorums = []
    for shard_server in config_state["shardServers"]:
        if shard_server["endpoint"] == endpoint:
            for quorum_info in shard_server["quorumInfos"]:
                quorums.append(quorum_info["quorumID"])
    return quorums

def pad(id):
    return "%011d" % int(id)    

def add_job(client, name, code_file, description, quorum_id, table_id):
    config = json.loads(client.get_json_config_state())
    database_name = "mapreduce_job_" + name
    database_id = client.create_database(database_name)
    client.use_database(database_name)
    client.create_table(database_id, quorum_id, "meta")
    client.create_table(database_id, quorum_id, "map_jobs")
    client.create_table(database_id, quorum_id, "reduce")
    client.create_table(database_id, quorum_id, "result")    
    client.use_table("meta")
    f = open(code_file)
    client.set("code", f.read())
    client.set("description", description)
    client.use_table("map_jobs")
    client.begin()
    for shard in config["shards"]:
        # TODO split ranges when there are multiple replicas
        quorum_name = "active:" + pad(shard["quorumID"])
        job = {}
        job["firstKey"] = shard["firstKey"]
        job["lastKey"] = shard["lastKey"]
        job["tableID"] = table_id
        client.set(quorum_name, json.dumps(job))
    client.submit()
    client.use("mapreduce", "jobs")
    client.set(name, "")

def daemon(client, endpoint):
    # mapreduce
    # - jobs table
    # mapreduce_job_X database
    # - map_jobs table
    # - meta table
    #   - code key
    # - 
    while True:
        try:
            config_state = json.loads(client.get_json_config_state())
            client.use("mapreduce", "jobs")
            jobs = client.list_keys("", "", 1, 0)
            if len(jobs) != 1:
                time.sleep(1)
                continue
            job_database = "mapreduce_job_" + jobs[0]
            print("Using job database " + job_database)
            client.use(job_database, "meta")
            code_text = client.get("code")
            eval(compile(code_text, "<string>", "exec"), globals(), locals())
            desc = client.get("description")
            if desc != None:
                print("Job description: " + desc)
            client.use(job_database, "map_jobs")
            quorums = local_quorums(config_state, endpoint)
            map_jobs = []
            for quorum_id in quorums:
                start_key = pad(quorum_id)
                end_key = pad(quorum_id + 1)
                quorum_jobs = client.list_keys(start_key, end_key)
                map_jobs.extend(quorum_jobs)
            print("Map jobs: " + str(map_jobs))
            for mj_key in map_jobs:
                mj_json = client.get(mj_key)
                mj_data = json.loads(mj_json)
                print("Trying map job " + mj_key + ", owner: " + mj_data.get("owner", "None"))
                if mj_data.get("owner", None) != None and mj_data.get("owner", None) != endpoint:
                    continue
                # obtain 'lock' on job
                mj_data["owner"] = endpoint
                new_json = json.dumps(mj_data)
                ret = client.test_and_set(mj_key, mj_json, new_json)
                if ret != new_json:
                    continue
                table_id = mj_data["tableID"]
                first_key = mj_data["firstKey"]
                last_key = mj_data["lastKey"]
                client.use_table_id(table_id)
                map_shard(client, first_key, last_key, mapred)
                client.delete(mj_key)
                print("Map job done: " + mj_key)
            print("All jobs done")
            time.sleep(1)
        except scaliendb.Error as e:
            print(e)
            continue
