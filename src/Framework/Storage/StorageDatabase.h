#ifndef STORAGEDATABASE_H
#define STORAGEDATABASE_H

#include "StorageShard.h"

/*
===============================================================================

 StorageDatabase

===============================================================================
*/

class StorageDatabase
{
public:
	void					Open(const char* dbName);
	
	StorageShard*			GetTable(const char* tableName);
	void					CloseTable(const char* tableName);
	void					Close();
	
	void					Commit(bool recovery = true, bool flush = true);
	
private:
	Buffer					name;
	InList<StorageShard>	tables;
};

#endif
