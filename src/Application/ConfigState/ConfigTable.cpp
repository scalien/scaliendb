#include "ConfigTable.h"


ConfigTable::ConfigTable()
{
    prev = next = this;
}

ConfigTable::ConfigTable(const ConfigTable& other)
{
    *this = other;
}

ConfigTable& ConfigTable::operator=(const ConfigTable& other)
{
    databaseID = other.databaseID;
    tableID = other.tableID;
    name = other.name;
    
    shards = other.shards;
    
    prev = next = this;
    
    return *this;
}
