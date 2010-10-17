#include "ConfigDatabase.h"

ConfigDatabase::ConfigDatabase()
{
    prev = next = this;
}

ConfigDatabase::ConfigDatabase(const ConfigDatabase& other)
{
    *this = other;
}

ConfigDatabase& ConfigDatabase::operator=(const ConfigDatabase& other)
{
    uint64_t    *dit;
    
    databaseID = other.databaseID;
    name = other.name;
    
    for (dit = other.tables.First(); dit != NULL; dit = other.tables.Next(dit))
        tables.Append(*dit);
    
    prev = next = this;
    
    return *this;
}