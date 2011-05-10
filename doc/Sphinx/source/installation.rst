.. _installation:


************
Installation
************

Installing on Debian Linux
==========================

Download the latest Debian package of ScalienDB from http://scalien.com. Once you have downloaded the ``.deb`` file, issue the following command to install ScalienDB::

  $ dpkg -i scaliendb-server_XXX_i386.deb

This will install the server and the proper ``init.d`` scripts. Launch ScalienDB using::

  $ /etc/init.d/scaliendb start

The usual ``start``, ``stop``, ``restart`` commands are available. Note that ScalienDB either runs as a controller or as a shardserver and thus requires a configuration file, please see section :ref:`configuration`.

Installing from source on Linux and other UNIX platforms
========================================================

Download the latest tarball of ScalienDB from http://scalien.com/downloads or directly from the commnad line (replace XXX with the latest version of ScalienDB)::

  $ wget http://scalien.com/releases/scaliendb/scaliendb-XXX.tgz

Once you have downloaded the ``tgz`` file, issue the following command to extract it::

  $ tar xvf scaliendb-XXX.tgz

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
