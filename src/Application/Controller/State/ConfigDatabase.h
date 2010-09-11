#ifndef CONFIGDATABASE_H
#define CONFIGDATABASE_H

#include "System/Common.h"
#include "System/Containers/InList.h"
#include "System/Buffers/Buffer.h"
#include "ConfigTable.h"


/*
===============================================================================

 ConfigDatabase

===============================================================================
*/

class ConfigDatabase
{
public:
	typedef InList<ConfigTable> TableList;

	uint64_t		databaseID;
	Buffer			name;
	
	TableList		tables;
	
	ConfigDatabase*	prev;
	ConfigDatabase*	next;
};

#endif
