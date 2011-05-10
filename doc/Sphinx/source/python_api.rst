.. _python_api:


**********
Python API
**********

Installing from source on Linux and other UNIX platforms
========================================================

You will need the ``python-config`` program to make the Python libraries. To check if you have it, type::

  $ which python-config

If not, you must install the Pyhton dev packages. On Debian, type::

  $ sudo apt-get install python-dev

On Redhat-like systems, type::

  $ sudo yum install python-devel

Note that on some systems, the name of the package may have the Python version number appended. In this case you can specify it as an argument to make eg. ``make pythonlib PYTHON_CONFIG=python2.4-config``.

Once you have verified you have ``python-config`` installed, make the Python client libraries::

  $ make pythonlib

in the ScalienDB folder. This will create the files ``_scaliendb_client.so``, ``scaliendb_client.py`` and ``scaliendb.py`` in ``bin/python``. Copy these to your Python project, and you are ready to use ScalienDB!

Installing from source on Windows
=================================

Currently not supported.

Connecting to the ScalienDB cluster
===================================

First, import the ScalienDB client library::

  import scaliendb

client.connect(controllers)
---------------------------

Then, create a client object by specifying the **controller nodes and their respective SDBP ports**. The SDBP port is 7080 by default and may be changed using ``sdbp.port`` setting in the configuration file::

  client = scaliendb.Client(["192.168.1.1:7080",
                             "192.168.1.2:7080",
                             "192.168.1.3:7080"])

Return values
=============

All ScalienDB functions return ``None`` on failure.

Setting timeout values
======================

Next, if you would like to, change the global timeout. The global timeout specifies the maximum time, in msec, that a ScalienDB client call will block your application. The default is 120 seconds::

  client.set_global_timeout(120*1000)

Next, if you would like to, change the master timeout. The master timeout specifies the maximum time, in msec, that the library will spend trying to find the master node. The default is 21 seconds::

  client.set_master_timeout(21*1000)

Schema commands
===============

Creating and deleting quroums
-----------------------------

client.create_quorum(list of nodeIDs)
-------------------------------------

Creates a quorum consisting from the passed in nodeIDs. Returns the new quorum's ID.

client.delete_quorum(quorumID)
------------------------------

Deletes the specified quorum.

Example::

  quorumID = client.create_quorum([100, 101]) # create a new quorum from nodes 100 and 101
  ...
  client.delete_quorum(quorumID) # delete the quorum we just created

Databases
---------

client.create_database(database name)
-------------------------------------

Create database with given name. Returns the new database's ID.

client.rename_database(old name, new name)
------------------------------------------

Rename database to a new name.

client.delete_database(name)
----------------------------

Deletes database.

client.use_database(database name)
----------------------------------

Use the database. Table-related commands will operate in this database.

Example::

  client.create_database("test")
  client.use_database("test")
  ...
  client.rename.database("test", "foo")
  ...
  client.delete_database("foo")

Tables
------

Don't forget to ``use_database`` before issuing table commands.

client.create_table(quorum ID, table name)
------------------------------------------

Create a new table with table name, and place its first shard into that quorum.

client.rename_table(old name, new name)
---------------------------------------

Rename table to new name.

client.delete_table(table name)
-------------------------------

Delete table.

client.truncate_table(table name)
---------------------------------

Truncate table. This deletes all key-values from the table (deletes all shards), and creates a new, empty shard for the table.

client.use_table(table name)
----------------------------

USe the table. Data commans like ``set`` and ``get`` will operate in this table.

Example::

  client.use_database("testdb")
  client.create_table("test")
  ...
  client.rename_table("test", "foo")
  ...
  client.delete_table("foo")

Writing into tables
===================

Before writing into tables, you have to select a database and a table, using ``use_database(database name)`` and ``use_table(table name)`` or select both at once using ``use(database name, table name)``.


client.set(key, value)
----------------------

Sets ``key => value``.

client.set_if_not_exists(key, value)
------------------------------------

Sets ``key => value`` is ``key`` is not in the table.

client.test_and_set(key, test, value)
-------------------------------------

Sets ``key => value`` if ``key => test`` currently.

client.get_and_set(key, value)
------------------------------

Sets ``key => value`` but returns the previous value.

client.add(key, value)
----------------------

Interprets the old value of ``key`` as an integer and adds ``value`` to it. For example, if ``key => -5`` and you issue ``add(key, 10)`` then ``key`` will be ``key => 5``.

client.delete(key)
------------------

Deletes ``key``.

client.remove(key)
------------------

Deletes ``key`` but returns its old value.

Reading from tables
===================

client.get(key)
---------------

Returns the value of ``key``.

List commands
=============

There are two list commands: ``list_keys`` and ``list_key_values`` and one ``count`` command, all have the same set of parameters.

client.list_keys(start key, end key, count, offset)
---------------------------------------------------

Listing starts at ``start key``, ends at ``end key`` (defaults to empty string). At most ``count`` elements are returned (default 0, which is infinity). Listing can be offset by ``offset`` elements.

client.list_keyvalues(start key, end key, count, offset)
--------------------------------------------------------

Listing starts at ``start key``, ends at ``end key`` (defaults to empty string). At most ``count`` elements are returned (default 0, which is infinity). Listing can be offset by ``offset`` elements.

client.count(start key, end key, count, offset)
-----------------------------------------------

Listing starts at ``start key``, ends at ``end key`` (defaults to empty string). At most ``count`` elements are returned (default 0, which is infinity). Listing can be offset by ``offset`` elements.

Batch writing
=============

For maximum thruput performance, it is possible to issue many write commands together; this is called batched writing. It will be faster then issuing single write commands because

#. The ScalienDB cluster will replicate them together
#. The client library will not wait for the previous' write commands response before send the next write command (saves rount-trip times).

In practice batched ``set`` can achieve 5-10x higher throughput than single ``set``.

To send batched write commands, first call ``begin()`` function, then issue the write commands, and finally call ``submit()``. The commands are sent on ``submit()``::

  client.begin()
  client.set("a1", "a1_value")
  client.set("a2", "a2_value")
  ...
  client.set("a99", "a99_value")
  client.submit() # commands are sent in batch

Bulk loading
============

Bulk loading sends the data directly to all nodes in the cluster bypassing the built-in ScalienDB replication. It is much faster then normal, consistent operation, but use it with care.

client.set_bulk_loading(True)
-----------------------------

Turn on bulk loading. Don't forget to turn it off once you are done.

Header files
============

Check out ``src/Application/ScalienDB/Client/Python/scaliendb.py`` for a full reference!

