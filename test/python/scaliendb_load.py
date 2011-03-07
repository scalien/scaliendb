import time
import scaliendb

def sizeof_fmt(num):
	for x in ['bytes','KB','MB','GB','TB']:
		if num < 1024.0:
			return "%3.1f%s" % (num, x)
		num /= 1024.0

class loader:
	def __init__(self):
		self.client = None
		self.granularity = 0
		self.print_stats = False
		self.items_batch = 0
		self.items_total = 0
		self.bytes_batch = 0
		self.bytes_total = 0
		self.starttime = 0
		self.endtime = 0

	def begin(self):
		self.starttime = time.time()
		self.client.begin()
		self.items_batch = 0
		self.bytes_batch = 0
	
	def submit(self):
		self.client.submit()
		self.endtime = time.time()
		if self.print_stats:
			endtimestamp = time.strftime("%H:%M:%S", time.gmtime())
			print("%s:\t Total bytes: %s \t total items: %i \t rps = %.0f \t bps = %s/s" % (endtimestamp, sizeof_fmt(self.bytes_total), self.items_total, (self.items_batch/((self.endtime - self.starttime) * 1000.0) * 1000.0), sizeof_fmt(self.bytes_batch/((self.endtime - self.starttime) * 1000.0) * 1000.0)))

	def set(self, key, value):
		l = len(key) + len(value)
		self.client.set(key, value)
		self.items_batch += 1
		self.items_total += 1
		self.bytes_batch += l
		self.bytes_total += l
		if self.bytes_batch > self.granularity:
			self.submit()
			self.begin()

def scaliendb_loader(client, granularity = 1000*1000, print_stats = False):
	lr = loader()
	lr.client = client
	lr.granularity = granularity
	lr.print_stats = print_stats
	return lr
