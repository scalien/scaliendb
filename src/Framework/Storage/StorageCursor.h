#ifndef STORAGECURSOR_H
#define STORAGECURSOR_H

#include "StorageDataPage.h"

class StorageTable;	// forward
class StorageDataPage;	// forward

/*
===============================================================================

 Catalog

===============================================================================
*/

class StorageCursor
{
public:
	StorageCursor(StorageTable* catalog);
	
	KeyValue*			Begin(ReadBuffer& key);
	KeyValue*			Next();
	void				Close();
	
	StorageCursor*		next;
	StorageCursor*		prev;
	
private:
	KeyValue*			FromNextPage();

	StorageTable*		catalog;
	StorageDataPage*	dataPage;
	KeyValue*			current;
	Buffer				nextKey;
	
	friend class StorageDataPage;
};

#endif
