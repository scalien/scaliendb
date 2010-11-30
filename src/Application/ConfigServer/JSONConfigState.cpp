#include "JSONConfigState.h"
#include "System/Macros.h"

#define JSON_STRING(obj, member) \
    json.PrintString(#member); \
    json.PrintColon(); \
    json.PrintString(obj->member);
    
#define JSON_NUMBER(obj, member) \
    json.PrintString(#member); \
    json.PrintColon(); \
    json.PrintNumber(obj->member);

#define JSON_IDLIST(obj, member) \
    json.PrintString(#member); \
    json.PrintColon(); \
    WriteIDList(obj->member);


JSONConfigState::JSONConfigState(ConfigState& configState_, JSONSession& json_) :
 configState(configState_),
 json(json_)
{
}

void JSONConfigState::Write()
{
    json.Start();
    
    WriteQuorums();
    json.PrintComma();
    WriteDatabases();
    json.PrintComma();
    WriteTables();
    json.PrintComma();
    WriteShards();
    json.PrintComma();
    WriteShardServers();
    
    // json.End() is called when the session is flushed
}
    
void JSONConfigState::WriteQuorums()
{
    ConfigQuorum*   quorum;
    
    json.PrintString("quorums");
    json.PrintColon();
    json.PrintArrayStart();

    FOREACH(quorum, configState.quorums)
        WriteQuorum(quorum);

    json.PrintArrayEnd();
}

void JSONConfigState::WriteQuorum(ConfigQuorum* quorum)
{
    json.PrintObjectStart();
    
    json.PrintString("quorumID");
    json.PrintColon();
    json.PrintNumber(quorum->quorumID);
    json.PrintComma();

    json.PrintString("hasPrimary");
    json.PrintColon();
    if (quorum->hasPrimary)
        json.PrintString("true");
    else
        json.PrintString("false");
    json.PrintComma();

    if (quorum->hasPrimary)
    {
        json.PrintString("primaryID");
        json.PrintColon();
        json.PrintNumber(quorum->primaryID);
        json.PrintComma();
    }
    
    json.PrintString("paxosID");
    json.PrintColon();
    json.PrintNumber(quorum->paxosID);
    json.PrintComma();

    json.PrintString("activeNodes");
    json.PrintColon();
    WriteIDList(quorum->activeNodes);
    json.PrintComma();

    json.PrintString("inactiveNodes");
    json.PrintColon();
    WriteIDList(quorum->inactiveNodes);
    json.PrintComma();

    json.PrintString("shards");
    json.PrintColon();
    WriteIDList(quorum->shards);

    json.PrintObjectEnd();
}

void JSONConfigState::WriteDatabases()
{
    ConfigDatabase*   database;
    
    json.PrintString("databases");
    json.PrintColon();
    json.PrintArrayStart();

    FOREACH(database, configState.databases)
        WriteDatabase(database);

    json.PrintArrayEnd();
}

void JSONConfigState::WriteDatabase(ConfigDatabase* database)
{
    json.PrintObjectStart();
    
    JSON_NUMBER(database, databaseID);
    json.PrintComma();
    JSON_STRING(database, name);
    json.PrintComma();
    JSON_IDLIST(database, tables);
    
    json.PrintObjectEnd();
}

void JSONConfigState::WriteTables()
{
    ConfigTable*   table;
    
    json.PrintString("tables");
    json.PrintColon();
    json.PrintArrayStart();

    FOREACH(table, configState.tables)
        WriteTable(table);

    json.PrintArrayEnd();
}

void JSONConfigState::WriteTable(ConfigTable* table)
{
    json.PrintObjectStart();
    
    JSON_NUMBER(table, tableID);
    json.PrintComma();
    JSON_NUMBER(table, databaseID);
    json.PrintComma();
    JSON_STRING(table, name);
    json.PrintComma();
    JSON_IDLIST(table, shards);
    
    json.PrintObjectEnd();
}

void JSONConfigState::WriteShards()
{
    ConfigShard*   shard;
    
    json.PrintString("shards");
    json.PrintColon();
    json.PrintArrayStart();

    FOREACH(shard, configState.shards)
        WriteShard(shard);

    json.PrintArrayEnd();
}

void JSONConfigState::WriteShard(ConfigShard* shard)
{
    json.PrintObjectStart();
    
    JSON_NUMBER(shard, shardID);
    json.PrintComma();
    JSON_NUMBER(shard, quorumID);
    json.PrintComma();
    JSON_NUMBER(shard, databaseID);
    json.PrintComma();
    JSON_NUMBER(shard, tableID);
    json.PrintComma();
    JSON_STRING(shard, firstKey);
    json.PrintComma();
    JSON_STRING(shard, lastKey);
    json.PrintComma();
    JSON_NUMBER(shard, shardSize);
    json.PrintComma();
    JSON_STRING(shard, splitKey);


    json.PrintObjectEnd();
}

void JSONConfigState::WriteShardServers()
{
    ConfigShardServer*   shardServer;
    
    json.PrintString("shardServers");
    json.PrintColon();
    json.PrintArrayStart();

    FOREACH(shardServer, configState.shardServers)
        WriteShardServer(shardServer);

    json.PrintArrayEnd();
}

void JSONConfigState::WriteShardServer(ConfigShardServer* server)
{
    json.PrintObjectStart();

    JSON_NUMBER(server, nodeID);
    json.PrintComma();

    json.PrintString("endpoint");
    json.PrintColon();
    json.PrintString(server->endpoint.ToReadBuffer());
    
    json.PrintObjectEnd();
}

template<typename List>
void JSONConfigState::WriteIDList(List& list)
{
    uint64_t*   it;
    
    json.PrintArrayStart();
    FOREACH(it, list)
    {
        if (it != list.First())
            json.PrintComma();
        json.PrintNumber(*it);
    }
    json.PrintArrayEnd();
}
