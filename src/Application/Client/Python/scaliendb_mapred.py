import scaliendb
from scaliendb_client import *

class MapReduce:
    def clear(self):
        pass
        
    def map(self, k, v):
        pass
    
    def reduce(self, ):
        pass

class FindStartsWith(MapReduce):
    def __init__(self, starts_with):
        self.results = []
        self.starts_with = starts_with
    
	def clear(self):
		self.results = []
		
    def map(self, k, v):
        if v.find(self.starts_with) == 0:
            self.results.append((k, v))
    
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
