#ifndef STORAGECURSOR_H
#define STORAGECURSOR_H

#include "StorageDataPage.h"

class StorageCatalog; // forward

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
	
private:
	KeyValue*			FromNextPage();

	StorageCatalog*		catalog;
	StorageDataPage*	dataPage;
	KeyValue*			current;
	Buffer				nextKey;
	
	friend class Catalog;
};

#endif
