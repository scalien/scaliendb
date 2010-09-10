#ifndef STORAGECURSOR_H
#define STORAGECURSOR_H

#include "StorageDataPage.h"

class StorageShard;		// forward
class StorageDataPage;	// forward

/*
===============================================================================

 StorageCursor

===============================================================================
*/

class StorageCursor
{
public:
	StorageCursor(StorageShard* shard);
	
	StorageKeyValue*		Begin(ReadBuffer& key);
	StorageKeyValue*		Next();
	void					Close();
	
	StorageCursor*			next;
	StorageCursor*			prev;
	
private:
	StorageKeyValue*		FromNextPage();

	StorageShard*			shard;
	StorageDataPage*		dataPage;
	StorageKeyValue*		current;
	Buffer					nextKey;
	
	friend class StorageDataPage;
};

#endif
