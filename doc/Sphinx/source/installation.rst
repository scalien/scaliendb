.. _installation:


************
Installation
************

Installing from source on Linux and other UNIX platforms
========================================================

Download the latest tarball of ScalienDB from github at http://github.com/scalien/scaliendb::

  $ git pull https://scalien@github.com/scalien/scaliendb.git scaliendb

This will create a directory called ``scaliendb``, which contains the source code and make files::

  $ cd scaliendb
  $ make

This will build the ScalienDB executable appropriate for your system. ScalienDB has no outside dependencies, so you're ready to rock and roll at this point.

To run ScalienDB::

  $ bin/scaliendb <configuration file>

The usual ``start``, ``stop``, ``restart`` commands are available. Note that ScalienDB either runs as a controller or as a shardserver and thus requires a configuration file, please see section :ref:`configuration`.

Installing on Windows
=====================

ScalienDB builds and runs of Windows, but there is no official Windows support at this time.
