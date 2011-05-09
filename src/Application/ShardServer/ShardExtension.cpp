#include "ShardExtension.h"
#include "ShardDatabaseManager.h"

static ShardDatabaseManager*    databaseManager;

void ShardExtensionSetManager(ShardDatabaseManager* manager_)
{
    databaseManager = manager_;
}

void ShardExtensionRegisterFunction(const char* name, ShardExtensionFunction func)
{
    // TODO:
    //databaseManager->RegisterExtension(name, func);
}

void ShardExtensionUnregisterFunction(const char* name)
{
    // TODO:
    //databaseManager->UnregisterExtension(name);
}
