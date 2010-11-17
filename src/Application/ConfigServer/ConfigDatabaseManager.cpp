#include "ConfigDatabaseManager.h"
#include "System/Config.h"

void ConfigDatabaseManager::Init()
{
    environment.InitCache(configFile.GetIntValue("database.cacheSize", STORAGE_DEFAULT_CACHE_SIZE));
    environment.Open(configFile.GetValue("database.dir", "db"));
    database = environment.GetDatabase("system");
    
    paxosID = 0;
    configState.Init();
    Read();
}

void ConfigDatabaseManager::Shutdown()
{
    environment.Close();
}

StorageDatabase* ConfigDatabaseManager::GetDatabase()
{
    return database;
}

ConfigState* ConfigDatabaseManager::GetConfigState()
{
    return &configState;
}

void ConfigDatabaseManager::SetPaxosID(uint64_t paxosID_)
{
    paxosID = paxosID_;
}

uint64_t ConfigDatabaseManager::GetPaxosID()
{
    return paxosID;
}

void ConfigDatabaseManager::Read()
{
    bool                ret;
    ReadBuffer          value;
    int                 read;
    
    ret =  true;
    
    if (!database->GetTable("config")->Get(ReadBuffer("state"), value))
    {
        Log_Message("Starting with empty database...");
        return;
    }
    
    read = value.Readf("%U:", &paxosID);
    if (read < 2)
        ASSERT_FAIL();
    
    value.Advance(read);
    
    if (!configState.Read(value))
        ASSERT_FAIL();

    Log_Trace("%R", &value);
}

void ConfigDatabaseManager::Write()
{
    writeBuffer.Writef("%U:", paxosID);
    configState.Write(writeBuffer);
    database->GetTable("config")->Set(ReadBuffer("state"), ReadBuffer(writeBuffer));
}
