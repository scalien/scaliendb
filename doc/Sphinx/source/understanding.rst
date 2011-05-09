.. _understanding:


***********************
Understanding ScalienDB
***********************

Controllers
-----------

The controller nodes act as the "brain" of the ScalienDB cluster and communicate among themselves to ensure the fault-tolerance and high-availability of the entire distributed database system. Shard servers are managed by the controllers to achieve linear read and write scalability and failover in case a shard server goes down. Internally, the controller nodes use the Paxos protocol to guarantee that the ScalienDB cluster is always in a well-defined, consistent state without the possibility of a "split-brain" scenario. One of the controller nodes acts as the master, which simplifies management tasks for the users and administrators of the database. The controllers contain the following modules:

- manage cluster membership of shard servers
- assign shard servers into quorums for replication
- store the database schema (databases, tables) using Paxos to protect against "split-brain" scenarios
- migrate shards across the cluster for manual or dynamic load and storage balancing

It is possible to use one, two, three or more controller nodes in the ScalienDB setup depending on the level of fault-tolerance desired by the customer use-case. It is recommended to use one controller node in test environments and three or five nodes in production. 

Shard servers
-------------

The shard servers are the workhorses of the distributed ScalienDB architecture. The shard servers, like the controllers, use a variant of the Paxos algorithm to ensure that the database is always in a consistent, well-defined state. The use of Paxos throughout the ScalienDB system guarantees single ACID and read-your-write semantics. Shard servers are assigned into quorums by the controllers for replication. The shard servers use a high-performance linear on-disk storage engine hand-crafted for ScalienDB to store user data in a compact way. The ScalienDB storage engine is the best of both worlds, as it combines technologies from traditional architectures such as transactional redo logging to eliminate any possibility of data loss and cutting-edge technologies such as linearized disk I/O introduced by Google's Bigtable system. The ScalienDB runs exceptionally well both on traditional spinning disk media and new solid state drives (SSDs). This combination of technologies assures that ScalienDB delivers both the dependability of old-school databases while opening the door to unlimited data and throughput scaling on conventional commodity hardware running Linux.

Clients
-------

The client library is a fully featured "smart client" that makes it easy for the programmer to quickly write ScalienDB-based applications without worrying about the distributed nature of the database. The client is fully aware of the state of the ScalienDB cluster and hides any failover or shard migration events from the user for maximum conveniance and minimal development overhead. The client supports distributing reads across quorum members for linear read scalability and distributing writes to different quorums for linear write scalability. Additionally, writes may be sent in a batched manner in case of bulk loading for maximum throughput. ScalienDB client libraries are available for Java, Python, PHP and C++, with each client library politely adhering to the idioms of its language for maximum programmer friendliness.

Data model
----------

ScalienDB is a NoSQL database with a key-value data model. At the highest level, data is organized into databases, then into tables, just like in traditional SQL databases. Tables are namespaces for key-value data, which may be split into several shards, which are then mapped into quorums and stored on shard servers for scalability and durability. Keys and values in ScalienDB are not strongly typed, which allows for maximum flexibility when developing applications against our API. Keys are limited to 64K length, while values may be practically unlimited in length (2^32 bytes), which means ScalienDB can be used for practically all data management tasks. This architecture combines the best of relational and NoSQL models: it features databases and tables to organize data while maximizing the flexibility for user-data by the use key-values and avoiding any locking and other single points of failure.

Management console
------------------

Administrative tasks are performed on the web-based management console using a browser like Mozilla Firefox or Google Chrome. The management console is fully featured and one-click supports all ScalienDB operations such as:

- monitoring the cluster state
- monitoring catchup and shard migration progress
- create, rename, delete databases
- create, rename, delete, truncate tables
- create, delete quorums
- add, remove shard servers from quorums
- shard splitting and shard migration

ScalienDB has a built-in HTTP server for performing management operations. This allows administrators and developers to integrate these tasks into their broader management suite using the powerful ScalienDB JSON API. 

Additional reading
------------------

For more details see the whitepapers available at http://scalien.com/whitepapers.

Paxos is explained in Leslie Lamport's paper `Paxos made simple <http://www.google.com/search?client=opera&rls=en&q=paxos+made+simple&sourceid=opera&ie=utf-8&oe=utf-8>`_.
