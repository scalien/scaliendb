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
    databaseID = other.databaseID;
    name = other.name;
    
    tables = other.tables;
    
    prev = next = this;
    
    return *this;
}
