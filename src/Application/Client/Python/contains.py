class Contains(MapReduce):
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

mapred = Contains(client, "x")
