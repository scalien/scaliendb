import scaliendb

class list_keys_result:
	def __init__(self):
		self.client = None
		self.start_key = ""
		self.count = 0
		self.granularity = 1000
		self.keys = []
		self.pos = 0
		self.total = 0
		self.offset = 0

	def __iter__(self):
		return self

	def next(self):
		if self.count > 0 and self.total == self.count:
			raise StopIteration
		if self.keys is not None:
			self.pos += 1
			if self.pos < len(self.keys):
				self.start_key = self.keys[self.pos]
				self.offset = 1
				self.total += 1
				return self.keys[self.pos]
		self.keys = self.client.list_keys(self.start_key, self.granularity, self.offset)
		self.pos = 0
		if self.keys is []:
			raise StopIteration
		else:
			self.total += 1
			return self.keys[self.pos]

class list_keyvalues_result:
	def __init__(self):
		self.client = None
		self.start_key = ""
		self.count = 0
		self.granularity = 1000
		self.dict = {}
		self.keys = []
		self.pos = 0
		self.total = 0
		self.offset = 0

	def __iter__(self):
		return self

	def next(self):
		if self.count > 0 and self.total == self.count:
			raise StopIteration
		if self.keys is not None:
			self.pos += 1
			if self.pos < len(self.keys):
				self.start_key = self.keys[self.pos]
				self.offset = 1
				self.total += 1
				return self.keys[self.pos], self.dict[self.keys[self.pos]]
		self.dict = self.client.list_key_values(self.start_key, self.granularity, self.offset)
		self.keys = sorted(self.dict.keys())
		self.pos = 0
		if self.keys is {}:
			raise StopIteration
		else:
			self.total += 1
			return self.keys[self.pos], self.dict[self.keys[self.pos]]

def scaliendb_iter_keys(client, start_key = "", count = 0, granularity = 1000):
	lr = list_keys_result()
	lr.client = client
	lr.start_key = start_key
	lr.count = count
	lr.granularity = granularity
	return lr

def scaliendb_iter_keyvalues(client, start_key = "", count = 0, granularity = 1000):
	lr = list_keyvalues_result()
	lr.client = client
	lr.start_key = start_key
	lr.count = count
	lr.granularity = granularity
	return lr
