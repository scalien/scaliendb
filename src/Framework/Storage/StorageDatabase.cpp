#include "StorageDatabase.h"

void StorageDatabase::Open(const char* dbName)
{
	name.Write(dbName);
}

StorageTable* StorageDatabase::GetTable(const char* tableName)
{
	StorageTable* it;
	
	for (it = tables.First(); it != NULL; it = tables.Next(it))
	{
		if (it->GetName() == tableName)
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

void StorageDatabase::Commit(bool flush)
{
	StorageTable* it;
	
	for (it = tables.First(); it != NULL; it = tables.Next(it))
		it->CommitPhase1();
		
	if (flush)
		sync();

	for (it = tables.First(); it != NULL; it = tables.Next(it))
		it->CommitPhase2();

	if (flush)
		sync();

	for (it = tables.First(); it != NULL; it = tables.Next(it))
		it->CommitPhase3();

}
