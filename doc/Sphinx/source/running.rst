.. _running:


************************************
Running ScalienDB for the first time 
************************************

A ScalienDB cluster consists of controllers and shard servers.

ScalienDB takes one command argument: the configuration file. Note that all paths in the configuration file such as ``database.dir`` must either be absolute or relative to the working directory. So, if the ScalienDB executable is in ``/home/joe/scaliendb/bin``, the configuration file is in ``/home/joe/scaliendb/0/scaliendb.conf`` and the configuration file specifies ``database.dir = 0``, then you should probably launch ScalienDB from ``/home/joe/scaliendb`` using::

  $ bin/scaliendb 0/scaliendb.conf

Starting the controllers
========================

First, start the controller(s). When the controllers start, they elect a master controller, which is shown on the console. Note that for master election to be successful a majority of controller nodes have to be up and running. You should see something like this::

  2011-05-09 11:13:26.450566: ScalienDB v0.9.9 started as CONTROLLER -- DEBUG Build date: May  9 2011 10:53:04, Pid: 14753
  2011-05-09 11:13:26.451811: Opening readonly: test/control/0/db/chunks/chunk.00000000000000000004
  ...
  2011-05-09 11:13:26.452144: Replaying log segments...
  2011-05-09 11:13:26.452242: Replaying log segment test/control/0/db/logs/log.00000000000000000013...
  ...
  2011-05-09 11:13:26.452359: Replaying done.
  2011-05-09 11:13:26.452368: Checking for orphaned chunks...
  2011-05-09 11:13:26.452408: Checking done.
  2011-05-09 11:13:26.452420: Existing environment opened.
  2011-05-09 11:13:26.453759: Opening log segment 15
  2011-05-09 11:13:26.453844: My nodeID is 0
  ...
  2011-05-09 11:13:29.874038: Node 1 became the master

Starting the web-based-management console
=========================================

Once the controllers are up and running, you can connect to the cluster using the web-based management console. The management console is a Javascript application, so you can run it from anywhere. It is located in the ``webadmin`` directory of your ScalienDB distribution, so if you downloaded ScalienDB to ``/home/joe/scaliendb``, you should open::

  file:///home/joe/scaliendb/webadmin/index.html

in your browser. Supported browsers are Firefox, Chrome and Safari.

Alternatively, the management console may also be accessed through the built-in ScalienDB HTTP server. This serves files according to the ``http.documentRoot`` and ``http.staticPrefix`` configuration settings. By default, if a ScalienDB instance is listening on 192.168.1.1 port 8080 you should also be able to access the management console at::

  http://192.168.1.1:8080/webadmin

After opening the management console, you first have to specify the HTTP endpoint of one of the controllers. You don't have to specify the master here, as any controller will redirect to the master. So, if you have controllers listening on HTTP ``192.168.1.1:8080``, ``192.168.1.2:8080``, ``192.168.1.3:8080`` then you can connect to either one.

If this the first time you are starting the cluster, no shard servers will have connected at this time, the list of shard servers on the ``Dashboard`` tab will be empty.

Starting the shard servers
==========================

Now you can start the shard servers. As shard servers start, the master controller will assign nodeIDs to them, starting at 100. The shard servers will show up on the list of shard servers on the ``Dashboard`` tab of the management console.

Creating quorums
================

Now that the controllers and shard servers are running, it's time to create quorums. Quorums are the basic unit of replication, and consist of one or more shard servers.

To create a quorum, click the ``Quorums`` tab of the management console and then click ``Create new quorum``. Next, enter the node IDs of the shard servers you would like to have in this quorum, for example ``100, 101, 102`` creates a 3-way replicated quorum.

Creating a database and a table
===============================

Now that you have created a quorum, we can create databases and tables. Click the ``Schema`` tab of the management console and click ``Create new database`` to create a database. Then click ``Create new table`` to create a new table. ScaliendB tables are key-value namespaces and are split into into shards and when a new table is created its first shard is automatically created with it. When you create table, you have to specify which quorum the table's first shard should be placed in.

Once you have created the table, you can examine the shards by clicking the ``Show shards`` button.

Writing into the table
======================

Once you have created the table, let's try to set ``hello => world`` into it. Let's use the shard server's HTTP server for this. Suppose one of the shard servers in the quorum is 192.168.1.5 and is listening on HTTP port 8080. Then we can use our browser to set::

  http://192.168.1.5:8080/set?tableID=1&key=hello&value=world

Here ``tableID`` is the ID of the table we just created, it starts at 1 and may be looked up under the ``Schema`` tab of the management console.

Alternatively, we can use the ScalienDB command line interface (CLI), which required Python. To build the CLI, type::

  make cli

Then start the CLI::

  bin/cli

You should see something like this::

  Python 2.6.1 (r261:67515, Jun 24 2010, 21:47:49) 
  [GCC 4.2.1 (Apple Inc. build 5646)] on darwin
  Type "help", "copyright", "credits" or "license" for more information.

  =====================
  ScalienDB shell 0.9.9
  =====================

  This is a standard Python shell, enhanced with ScalienDB client library commands.
  Type "shelp" for help.

  >>> 

Now you have to connect to the controllers. The CLI uses the ScalienDB Protocotol (SDBP) to connect to the servers. By default, this port is 7080, but it can be changed in the configuration file using the ``sdbp.port`` setting. In the following example we connect to the 3-way replicated cluster from above, assume we have a database called ``testdb`` and inside it a table called ``testtable`` and set ``hello => world``::

  >>> connect(["192.168.1.1:7080", "192.168.1.2:7080", "192.168.1.3:7080"])
  >>> use_database("testdatabase")
  (0.01 secs)
  >>> use_table("testtable")
  (0.00 secs)
  >>> set("hello", "world")
  (0.01 secs)
  >>> get("hello")
  (0.00 secs)
  'world'
  >>> 

For more CLI commands, type ``shelp``. Use ``quit()`` or ``Ctrl-D`` to quit the CLI.