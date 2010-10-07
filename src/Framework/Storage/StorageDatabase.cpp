#include "StorageDatabase.h"
#include "StorageDataCache.h"
#include "System/FileSystem.h"

#include <stdio.h>

StorageDatabase::~StorageDatabase()
{
    Close();
}

void StorageDatabase::Open(const char* path_, const char* dbName)
{
    char    sep;
    
    environment = NULL;
    
    name.Write(dbName);
    name.NullTerminate();

    // try to create parent directory, will fail later when creating the database
    if (!FS_IsDirectory(path_))
        FS_CreateDir(path_);

    sep = FS_Separator();
    path.Append(path_);
    if (path.GetBuffer()[path.GetLength() - 1] != sep)
        path.Append(&sep, 1);
    
    // name is null-terminated
    path.Append(name.GetBuffer(), name.GetLength() - 1);
    if (path.GetBuffer()[path.GetLength() - 1] != sep)
        path.Append(&sep, 1);
    
    path.NullTerminate();
    if (!FS_IsDirectory(path.GetBuffer()))
    {
        if (!FS_CreateDir(path.GetBuffer()))
            ASSERT_FAIL();
    }   
}

const char* StorageDatabase::GetName()
{
    return name.GetBuffer();
}

StorageEnvironment* StorageDatabase::GetEnvironment()
{
    return environment;
}

StorageTable* StorageDatabase::GetTable(const char* tableName)
{
    StorageTable*   it;
    
    for (it = tables.First(); it != NULL; it = tables.Next(it))
    {
        if (MEMCMP(it->GetName(), strlen(it->GetName()), tableName, strlen(tableName)))
            return it;
    }
    
    it = new StorageTable();
    it->Open(path.GetBuffer(), tableName);
    it->database = this;
    tables.Append(it);
    
    return it;
}

void StorageDatabase::CloseTable(const char* tableName)
{
    StorageTable* it;
    
    for (it = tables.First(); it != NULL; it = tables.Next(it))
    {
        if (strcmp(it->GetName(), tableName) == 0)
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
    StorageTable*   it;
    long            el1, els1, el2, els2, el3, el4;
    Stopwatch       sw;

    el1 = els1 = el2 = els2 = el3 = el4 = 0;
    
    if (recovery)
    {
        sw.Start();
        for (it = tables.First(); it != NULL; it = tables.Next(it))
            it->CommitPhase1();
        sw.Stop();
        el1 = sw.Elapsed();

        sw.Restart();
        if (flush)
            FS_Sync();
        sw.Stop();
        els1 = sw.Elapsed();
    }

    sw.Restart();
    for (it = tables.First(); it != NULL; it = tables.Next(it))
        it->CommitPhase2();
    sw.Stop();
    el2 = sw.Elapsed();

    sw.Restart();
    if (flush)
        FS_Sync();
    sw.Stop();
    els2 = sw.Elapsed();

    if (recovery)
    {
        sw.Restart();
        for (it = tables.First(); it != NULL; it = tables.Next(it))
            it->CommitPhase3();
        sw.Stop();
        el3 = sw.Elapsed();
    }
    
    sw.Restart();
    for (it = tables.First(); it != NULL; it = tables.Next(it))
        it->CommitPhase4();
    sw.Stop();
    el4 = sw.Elapsed();

    Log_Trace("el1 = %ld, els1 = %ld, el2 = %ld, els2 = %ld, el3 = %ld, el4 = %ld\n",
        el1, els1, el2, els2, el3, el4);
}
