.. _configuration:


*************
Configuration
*************

Sample configuration files
==========================

The ScalienDB distribution includes sample configuration files as a starting point. The configuration files are called ``scaliendb.conf`` and are located in the::

  test/control/{single|0|1|2}/

and::

  test/shard/{0|1|2|...}

folders of the distribution. For example, the file ``test/control/single/scaliendb.conf`` is a stand-alone controller configuration file.

The rest of this chapter explains the options in the ScalienDB configuration file.

Configuring a controller
========================

To run ScalienDB as a controller, specify the ``role`` property::

  role = controller

Next, controllers must be assigned fixed node IDs starting from zero in the configuration file. For example::

  nodeID = 0

Next, you must specify the cluster endpoint of the controller. This consists of the IP address ScalienDB will bind to and the network port it will use to communicate with all other ScalienDB instances (controllers, shard servers). For example, to run on 192.168.1.1 on port 10000::

  endpoint = 192.168.1.1:10000

Next, you must specify the complete controller quorum. For example, if you are running the one controller from the previous example, write::

  controllers = 192.168.1.1:10000

If you are running several controllers, separate the controllers endpoints using commas like::

  controllers = 192.168.1.1:10000, 192.168.1.2:10000, 192.168.1.3:10000

Additional controllers provide additional fault-tolerance and high-availability.

Next, specify the database directory where all data files will be stored::

  database.dir = /var/lib/scaliendb

Sample configuration for a single controller
--------------------------------------------

Below we show a sample configuration file for a single ScalienDB controller::

  role = controller
  nodeID = 0
  endpoint = 192.168.1.1:10000
  controllers = 192.168.1.1:10000
  database.dir = /var/lib/scaliendb

Sample configuration for replicated controllers
-----------------------------------------------

Below we show a sample configuration file for a 3-way replicated ScalienDB controller.

First controller (node 0)::

  role = controller
  nodeID = 0
  endpoint = 192.168.1.1:10000
  controllers = 192.168.1.1:10000, 192.168.1.2:10000, 192.168.1.3:10000
  database.dir = /var/lib/scaliendb

Second controller (node 1)::

  role = controller
  nodeID = 1
  endpoint = 192.168.1.2:10000
  controllers = 192.168.1.1:10000, 192.168.1.2:10000, 192.168.1.3:10000
  database.dir = /var/lib/scaliendb

Third controller (node 2)::

  role = controller
  nodeID = 2
  endpoint = 192.168.1.3:10000
  controllers = 192.168.1.1:10000, 192.168.1.2:10000, 192.168.1.3:10000
  database.dir = /var/lib/scaliendb

Note that only the ``nodeID`` and ``endpoint`` settings change.

Configuring a shard server
==========================

To run ScalienDB as a shard server, specify the ``role`` property::

  role = shard

Next, you must specify the cluster endpoint of the shard server. This consists of the IP address ScalienDB will bind to and the network port it will use to communicate with all other ScalienDB instances (controllers, shard servers). For example, to run on 192.168.1.5 on port 10000::

  endpoint = 192.168.1.5:10000

Next, you must specify the cluster endpoint of the controller(s). This is the **same** ``controllers`` line as found in the configuration file of the controllers! For example, to connect the shard server to the 3-way replicated cluster above::

  controllers = 192.168.1.1:10000, 192.168.1.2:10000, 192.168.1.3:10000

Next, specify the database directory where all data files will be stored::

  database.dir = /var/lib/scaliendb

Note that the shard server configuration file does **not** require a node ID, as node IDs are automatically generated and assigned by the controllers to the shard servers!

Sample configuration for a shard server
---------------------------------------

Below we show a sample configuration file for a shard server connecting to a ScalienDB cluster with 3-way replicated controllers::

  role = shard
  endpoint = 192.168.1.5:10000
  controllers = 192.168.1.1:10000, 192.168.1.2:10000, 192.168.1.3:10000
  database.dir = /var/lib/scaliendb

To run another shard server on another machine, say 192.168.1.6, you would use the following configurationf file where only ``endpoint`` is changed::

  role = shard
  endpoint = 192.168.1.6:10000
  controllers = 192.168.1.1:10000, 192.168.1.2:10000, 192.168.1.3:10000
  database.dir = /var/lib/scaliendb

Comments
========

Lines in the configuration file beginning with ``#`` are treated as comments::

  # this is a comment

Required lines
==============

- ``role``

- ``endpoint``

- ``nodeID`` only required if ``role = controller``

- ``controllers``

Optional lines
==============

- ``database.dir`` defaults to ``db``

- ``log.trace`` defaults to ``false``

- ``log.targets`` defaults to ``stdout``

- ``log.timestamping`` defaults to ``false``

- ``log.file``

- ``http.documentRoot`` defaults to ``.``

- ``http.port`` defaults to 8080

- ``sdbp.port`` defaults to 7080

Client configuration
====================

When a client connects to a ScalienDB cluster, you have to tell the ScalienDB client library where to connect to. This is the connection string: the host name(s) and the port numbers. **Important:** You always tell the client library the ``controller`` line, that's where the client connects to! In the 3-way replicated example above, the the connection string would be::

  192.168.1.1:10000, 192.168.1.2:10000, 192.168.1.3:10000
