import random
random.seed()

class Server:
	def __init__(self, i):
		self.index = i
		self.ops = []
	def push_op(self, c):
		self.ops.append(c)
		c.waiting = True
		# print("Server %d: Pushing request of client %d" % (self.index, c.index))
	def pop_op(self):
		global num_served
		if len(self.ops) > 0:
			c = self.ops.pop()
			c.waiting = False
			# print("Server %d: Popping request of client %d" % (self.index, c.index))
			num_served += 1
	def tick(self):
		self.pop_op()

class Client:
	def __init__(self, i):
		self.index = i
		self.waiting = False
	def tick(self):
		global num_servers
		global servers
	 	if not self.waiting:
			r = random.randint(0, num_servers-1)
			# print("Client %d: sending to %d" % (self.index, r))
			servers[r].push_op(self)
		# else: print("Client %d: waiting..." % self.index)

def init():
	for i in xrange(num_servers): servers.append(Server(i))
	for i in xrange(num_clients): clients.append(Client(i))

def simulation():
	for i in xrange(num_ops):
		for j in xrange(num_clients): clients[j].tick()
		for j in xrange(num_servers): servers[j].tick()

num_clients = 3
num_servers = 3
num_ops = 10*1000
num_served = 0
servers = []
clients = []

init()
simulation()

print("num_clients = %d" % num_clients)
print("num_servers = %d" % num_servers)
print("num_ops = %d" % num_ops)
print("num_served = %d" % num_served)
scaling_factor = float(num_served) / num_ops
print("scaling factor = %f" % scaling_factor)
