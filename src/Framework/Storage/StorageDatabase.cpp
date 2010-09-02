#include "StorageDatabase.h"
#include "System/FileSystem.h"

void StorageDatabase::Open(const char* dbName)
{
	name.Write(dbName);
}

StorageTable* StorageDatabase::GetTable(const char* tableName)
{
	StorageTable* it;
	
	for (it = tables.First(); it != NULL; it = tables.Next(it))
	{
		if (MEMCMP(it->GetName(), strlen(it->GetName()), tableName, strlen(tableName)))
			return it;
	}
	
	it = new StorageTable();
	it->Open(tableName);
	tables.Append(it);
	
	return it;
}

void StorageDatabase::CloseTable(const char* tableName)
{
	StorageTable* it;
	
	for (it = tables.First(); it != NULL; it = tables.Next(it))
	{
		if (it->GetName() == tableName)
		{
			it->Close();
			tables.Delete(it);
			return;
		}
	}
}

void StorageDatabase::Close()
{
	StorageTable* it;
	
	for (it = tables.First(); it != NULL; it = tables.Delete(it))
		it->Close();
}

void StorageDatabase::Commit(bool recovery, bool flush)
{
	StorageTable* it;
	
	if (recovery)
	{
		for (it = tables.First(); it != NULL; it = tables.Next(it))
			it->CommitPhase1();

		if (flush)
			FS_Sync();
	}	

	for (it = tables.First(); it != NULL; it = tables.Next(it))
		it->CommitPhase2();

	if (flush)
		FS_Sync();

	if (recovery)
	{
		for (it = tables.First(); it != NULL; it = tables.Next(it))
			it->CommitPhase3();
	}
	
	for (it = tables.First(); it != NULL; it = tables.Next(it))
		it->CommitPhase4();
}
