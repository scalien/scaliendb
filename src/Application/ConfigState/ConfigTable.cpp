#include "ConfigTable.h"


ConfigTable::ConfigTable()
{
    prev = next = this;
    databaseID = 0;
    tableID = 0;
    isFrozen = true;
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
    isFrozen = other.isFrozen;
    
    shards = other.shards;
    
    prev = next = this;
    
    return *this;
}
