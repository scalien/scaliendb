#include "StorageDatabase.h"
#include "StorageDataCache.h"
#include "System/FileSystem.h"

#include <stdio.h>

StorageDatabase::StorageDatabase()
{
    prev = next = this;
}

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
            STOP_FAIL(1, "Cannot create database directory (%s)", path.GetBuffer());
    }   
}

const char* StorageDatabase::GetName()
{
    return name.GetBuffer();
}

uint64_t StorageDatabase::GetSize()
{
    StorageTable*   it;
    uint64_t        size;
    
    size = 0;
    FOREACH (it, tables)
        size += it->GetSize();
    
    return size;
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

bool StorageDatabase::DeleteTable(StorageTable* table)
{
    Buffer  tableName;
    Buffer  deletedName;
    char    sep;

    tables.Remove(table);

    table->Close();

    // TODO: recovery and error handling

    sep = FS_Separator();
    tableName.Write(path.GetBuffer(), path.GetLength() - 1);
    tableName.Append(&sep, 1);
    tableName.Append(table->GetName());
    tableName.NullTerminate();

    deletedName.Write(path.GetBuffer(), path.GetLength() - 1);
    deletedName.Append(&sep, 1);
    deletedName.Append("deleted");
    deletedName.Append(table->GetName());
    deletedName.NullTerminate();
    
    FS_Rename(tableName.GetBuffer(), deletedName.GetBuffer());

    delete table;

    return true;
}

void StorageDatabase::Commit(bool recovery, bool flush)
{    
    if (recovery)
    {
        CommitPhase1();
        if (flush)
            FS_Sync();
    }

    CommitPhase2();

    if (flush)
        FS_Sync();

    if (recovery)
        CommitPhase3();
    
    CommitPhase4();
}

void StorageDatabase::CommitPhase1()
{
    StorageTable*   it;

    for (it = tables.First(); it != NULL; it = tables.Next(it))
        it->CommitPhase1();
}

void StorageDatabase::CommitPhase2()
{
    StorageTable*   it;

    for (it = tables.First(); it != NULL; it = tables.Next(it))
        it->CommitPhase2();
}

void StorageDatabase::CommitPhase3()
{
    StorageTable*   it;

    for (it = tables.First(); it != NULL; it = tables.Next(it))
        it->CommitPhase3();
}

void StorageDatabase::CommitPhase4()
{
    StorageTable*   it;

    for (it = tables.First(); it != NULL; it = tables.Next(it))
        it->CommitPhase4();
}

