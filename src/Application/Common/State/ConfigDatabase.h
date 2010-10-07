#ifndef CONFIGDATABASE_H
#define CONFIGDATABASE_H

#include "System/Common.h"
#include "System/Containers/List.h"
#include "System/Buffers/Buffer.h"

#define CONFIG_DATABASE_PRODUCTION      'P'
#define CONFIG_DATABASE_TEST            'T'

/*
===============================================================================================

 ConfigDatabase

===============================================================================================
*/

class ConfigDatabase
{
public:
    ConfigDatabase();
    ConfigDatabase(const ConfigDatabase& other);
    
    ConfigDatabase&     operator=(const ConfigDatabase& other);

    uint64_t            databaseID;
    Buffer              name;
    
    List<uint64_t>      tables;
    
    ConfigDatabase*     prev;
    ConfigDatabase*     next;
};

inline ConfigDatabase::ConfigDatabase()
{
    prev = next = this;
}

inline ConfigDatabase::ConfigDatabase(const ConfigDatabase& other)
{
    *this = other;
}

inline ConfigDatabase& ConfigDatabase::operator=(const ConfigDatabase& other)
{
    uint64_t    *dit;
    
    databaseID = other.databaseID;
    name = other.name;
    
    for (dit = other.tables.First(); dit != NULL; dit = other.tables.Next(dit))
        tables.Append(*dit);
    
    prev = next = this;
    
    return *this;
}

#endif
