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

#endif
