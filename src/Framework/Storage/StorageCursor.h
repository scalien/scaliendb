#ifndef STORAGECURSOR_H
#define STORAGECURSOR_H

#include "StorageDataPage.h"

class StorageTable;     // forward
class StorageShard;     // forward
class StorageDataPage;  // forward

/*
===============================================================================

 StorageCursor

===============================================================================
*/

class StorageCursor
{
public:
    StorageCursor(StorageTable* table);
    StorageCursor(StorageShard* shard);
    
    void                    SetBulk(bool bulk);
    StorageKeyValue*        Begin(ReadBuffer& key);
    StorageKeyValue*        Next();
    void                    Close();
    
    StorageCursor*          next;
    StorageCursor*          prev;
    
private:
    StorageKeyValue*        FromNextPage();

    bool                    bulk;
    StorageTable*           table;
    StorageShard*           shard;
    StorageFile*            file;
    StorageDataPage*        dataPage;
    StorageKeyValue*        current;
    Buffer                  nextKey;
    
    friend class StorageDataPage;
    friend class StorageFile;
    friend class StorageShard;
    friend class StorageTable;
};

#endif
