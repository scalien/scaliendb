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

Note that on some systems, the name of the package may have the Python version number appended. In this case you can specify it as an argument to ``make``, eg. ``make pythonlib PYTHON_CONFIG=python2.4-config``.

Once you have verified you have ``python-config`` installed, make the Python client libraries::

  $ make pythonlib

in the ScalienDB folder. This will create the files ``_scaliendb_client.so``, ``scaliendb_client.py`` and ``scaliendb.py`` in the ``bin/python`` folder. Copy these to your Python project, and you are ready to use ScalienDB!

Installing from source on Windows
=================================

Currently not supported.

Client object
=============

Client(controllers)
-------------------

Create a client object by specifying the **controller nodes and their respective SDBP ports**. (SDBP stands for ScalienDB Protocol.) The SDBP port is 7080 by default and may be changed using the ``sdbp.port`` setting in the server's configuration file::

  client = scaliendb.Client(["192.168.1.1:7080",
                             "192.168.1.2:7080",
                             "192.168.1.3:7080"])

client.set_global_timeout(timeout)
----------------------------------

The global timeout specifies the maximum time, in msec, that a ScalienDB client call will block your application. The default is 120 seconds::

  client.set_global_timeout(120*1000)

client.set_master_timeout(timeout)
----------------------------------

The master timeout specifies the maximum time, in msec, that the library will spend trying to find the master node. The default is 21 seconds::

  client.set_master_timeout(21*1000)

client.create_quorum(list of nodeIDs)
-------------------------------------

Creates a quorum consisting from the passed in nodeIDs. Returns the new quorum's ID::

  quorumID = client.create_quorum([100, 101])

client.delete_quorum(quorumID)
------------------------------

Deletes the specified quorum::

  client.delete_quorum(quorumID)

client.create_database(database name)
-------------------------------------

Create database with given name. Returns the new database's ID::

  database_id = client.create_database("test_database")

client.rename_database(old name, new name)
------------------------------------------

Rename database to a new name::

  client.rename_database("old_db", "new_db")

client.delete_database(name)
----------------------------

Deletes database::

  client.delete_database("test_database")

client.use_database(database name)
----------------------------------

Use the database. Table-related commands will operate in this database::

  client.use_database("test_database")
  client.use_table("test_table")
  client.set("key", "value")

client.create_table(table name)
------------------------------------------

Don't forget to ``use_database`` before issuing table commands. Create a new table with table name::

  client.use_database("test_database")
  client.create_table("test_table")

The table's first shard is placed into a random quorum.

client.create_table(table name, quorum_id)
------------------------------------------

Create a new table with table name, and place its first shard into that quorum::

  client.use_database("test_database")
  client.create_table("test_table", 1)

client.rename_table(old name, new name)
---------------------------------------

Rename table to new name::

  client.use_database("test_database")
  client.rename_table("old_table", "new_table")

client.delete_table(table name)
-------------------------------

Delete table::

  client.use_database("test_database")
  client.delete_table("test_table")

client.truncate_table(table name)
---------------------------------

Truncate table. This deletes all key-values from the table (deletes all shards), and creates a new, empty shard for the table::

  client.use_database("test_database")
  client.truncate_table("test_table")

client.use_table(table name)
----------------------------

Use the table. Table-related commands will operate in this table::

  client.use_database("test_database")
  client.use_table("test_table")
  client.set("key", "value")

client.freeze_table(table name)
-------------------------------

Freeze table. The shards making up this table will **not** be split automatically::

  client.use_database("test_database")
  client.freeze_table("test_table")

By default, all tables are unfrozen.

client.unfreeze_table(table name)
-------------------------------

Unfreeze table. The shards making up this table will be split automatically as they grow in size::

  client.use_database("test_database")
  client.unfreeze_table("test_table")

client.split_shard(shard id, key)
---------------------------------

Force splitting of shard into two shards at ``key``::

  client.split_shard(shard_id, "c")

client.get(key)
---------------

Returns the value of ``key``::

  client.use_database("test_database")
  client.use_table("test_table")
  client.set("foo", "bar")
  value = client.get("foo")
  print(value) # prints "bar"

client.set(key, value)
----------------------

Sets ``key => value``::

  client.use_database("test_database")
  client.use_table("test_table")
  client.set("key", "value")
  client.set(5, 55)
  client.set("now", datetime.now())

client.set_if_not_exists(key, value)
------------------------------------

Sets ``key => value`` is ``key`` is not in the table::

  client.use_database("test_database")
  client.use_table("test_table")
  client.set_if_not_exists("key", "value")

client.test_and_set(key, test, value)
-------------------------------------

Sets ``key => value`` if ``key => test`` currently::

  client.use_database("test_database")
  client.use_table("test_table")
  client.set("key", "test", "value")
  client.set("key", 55, "value")

client.get_and_set(key, value)
------------------------------

Sets ``key => value`` but returns the previous value::

  client.use_database("test_database")
  client.use_table("test_table")
  old_value = client.get_and_set("key", "new value")

client.add(key, value)
----------------------

Interprets the old value of ``key`` as an integer and adds ``value`` to it. Use this to implement counters for indexing objects. ::

  client.use_database("test_database")
  client.use_table("test_table")
  client.set("users", 0)
  user_id = client.add("users", 1) # returns 1
  user_id = client.add("users", 1) # returns 2
  user_id = client.add("users", 1) # returns 3
  ...
  
client.delete(key)
------------------

Deletes ``key``::

  client.use_database("test_database")
  client.use_table("test_table")
  client.delete("key")

client.remove(key)
------------------

  client.use_database("test_database")
  client.use_table("test_table")
  old_value = client.remove("key")

scaliendb.composite(values)
---------------------------

Returns a string representation of the values passed, to be used as an index key. For example, if we want to index tweets by user_id and datetime::

  client.use_database("twitter")
  client.use_table("index_tweets_datetime")
  client.set(scaliendb.composite(tweet["user_id"], tweet["datetime"]), tweet["tweet_id"])

For example, if the ``user_id`` is 55, the datetime is ``2011-06-02 18:00:35.296616`` and ``tweet_id`` is 33, this generates the key-value pair::

  /000000000000000000055/2011-06-02 18:00:35.296616 => 000000000000000000033

client.list_keys(start key, end key, prefix, count, offset)
-----------------------------------------------------------

Listing starts at ``start key``, ends at ``end key`` and only lists keys which start with ``prefix`` (all default to empty string). At most ``count`` elements are returned (default 0, which is infinity). Listing can be offset by ``offset`` elements::

  client.use_database("test_database")
  client.use_table("test_table")
  keys = client.list_keys(prefix="/abc", start_key="/def", count=1000)
  for key in keys:
    print(key)

client.list_keyvalues(start key, end key, prefix, count, offset)
----------------------------------------------------------------

Same as before, but returns ``key => value`` pairs in a dictionary.

Listing starts at ``start key``, ends at ``end key`` and only lists keys which start with ``prefix`` (all default to empty string). At most ``count`` elements are returned (default 0, which is infinity). Listing can be offset by ``offset`` elements::

  kvs = index_tweets_datetime.list_key_values(prefix=scaliendb.composite(55, "2011-01-01 00:00:00"), count=1000)
  for key, tweet_id in sorted(kvs.items()):
    tweet = loads(tweets.get(tweet_id))
    print(tweet)

client.count(start key, end key, prefix, count, offset)
-------------------------------------------------------

Same as before, but only returns the number of matching elements.

Listing starts at ``start key``, ends at ``end key`` and only lists keys which start with ``prefix`` (all default to empty string). At most ``count`` elements are returned (default 0, which is infinity). Listing can be offset by ``offset`` elements::

  client.use_database("test_database")
  client.use_table("test_table")
  count = client.count(prefix="/abc", start_key="/def", count=1000)
  print(count)

client.begin()
--------------

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

client.submit()
---------------

Sends the batched commands to the server. See previous example.

client.set_bulk_loading(True)
-----------------------------

  client.set_bulk_loading(True)
  client.begin()
  client.set("a1", "a1_value")
  client.set("a2", "a2_value")
  ...
  client.set("a99", "a99_value")
  client.submit() # commands are sent in batch
  client.set_bulk_loading(False)

Header files
============

Check out ``src/Application/ScalienDB/Client/Python/scaliendb.py`` for a full reference!

