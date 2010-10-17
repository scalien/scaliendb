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
    uint64_t    *sit;
    
    databaseID = other.databaseID;
    tableID = other.tableID;
    name = other.name;
    
    for (sit = other.shards.First(); sit != NULL; sit = other.shards.Next(sit))
        shards.Append(*sit);
    
    prev = next = this;
    
    return *this;
}
