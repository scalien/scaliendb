#ifndef STORAGECHUNKFILE_H
#define STORAGECHUNKFILE_H

#include "StorageHeaderPage.h"
#include "StorageIndexPage.h"
#include "StorageBloomPage.h"
#include "StorageDataPage.h"

/*
===============================================================================================

 StorageFile

===============================================================================================
*/

class StorageChunkFile
{
public:
    StorageChunkFile();
    ~StorageChunkFile();

    void                AppendDataPage(StorageDataPage* dataPage);

    StorageHeaderPage   headerPage;
    StorageIndexPage    indexPage;
    StorageBloomPage    bloomPage;

    unsigned            numDataPages;
    StorageDataPage**   dataPages;

private:
    void                ExtendDataPageArray();

    unsigned            size;
};

#endif
