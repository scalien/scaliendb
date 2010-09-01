#ifndef STORAGECURSOR_H
#define STORAGECURSOR_H

#include "StorageDataPage.h"

class StorageTable;	// forward
class StorageDataPage;	// forward

/*
===============================================================================

 StorageCursor

===============================================================================
*/

class StorageCursor
{
public:
	StorageCursor(StorageTable* table);
	
	StorageKeyValue*		Begin(ReadBuffer& key);
	StorageKeyValue*		Next();
	void					Close();
	
	StorageCursor*			next;
	StorageCursor*			prev;
	
private:
	StorageKeyValue*		FromNextPage();

	StorageTable*			table;
	StorageDataPage*		dataPage;
	StorageKeyValue*		current;
	Buffer					nextKey;
	
	friend class StorageDataPage;
};

#endif
