#ifndef STORAGECURSOR_H
#define STORAGECURSOR_H

#include "StorageDataPage.h"

class StorageCatalog;	// forward
class StorageDataPage;	// forward

/*
===============================================================================

 Catalog

===============================================================================
*/

class StorageCursor
{
public:
	StorageCursor(StorageCatalog* catalog);
	
	KeyValue*			Begin(ReadBuffer& key);
	KeyValue*			Next();
	void				Close();
	
	StorageCursor*		next;
	StorageCursor*		prev;
	
private:
	KeyValue*			FromNextPage();

	StorageCatalog*		catalog;
	StorageDataPage*	dataPage;
	KeyValue*			current;
	Buffer				nextKey;
	
	friend class StorageDataPage;
};

#endif
