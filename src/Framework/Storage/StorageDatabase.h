#ifndef STORAGEDATABASE_H
#define STORAGEDATABASE_H

#include "StorageTable.h"

class StorageDatabase
{
public:
	void					Open(const char* dbName);
	
	StorageTable*			GetTable(const char* tableName);
	void					CloseTable(const char* tableName);
	void					Close();
	
	void					Commit(bool flush = true);
	
private:
	Buffer					name;
	InList<StorageTable>	tables;
};

#endif
