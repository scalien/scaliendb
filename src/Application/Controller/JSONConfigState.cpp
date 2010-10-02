#include "JSONConfigState.h"

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
}
    
void JSONConfigState::WriteQuorums()
{
    ConfigQuorum*   quorum;
    
    json.PrintString(ReadBuffer("quorums"));
    json.PrintColon();
    json.PrintArrayStart();

    for (quorum = configState.quorums.First(); quorum != NULL; quorum = configState.quorums.Next(quorum))
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
    
    json.PrintString(ReadBuffer("databases"));
    json.PrintColon();
    json.PrintArrayStart();

    for (database = configState.databases.First(); database != NULL; database = configState.databases.Next(database))
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
    
    json.PrintString(ReadBuffer("tables"));
    json.PrintColon();
    json.PrintArrayStart();

    for (table = configState.tables.First(); table != NULL; table = configState.tables.Next(table))
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
    
    json.PrintString(ReadBuffer("shards"));
    json.PrintColon();
    json.PrintArrayStart();

    for (shard = configState.shards.First(); shard != NULL; shard = configState.shards.Next(shard))
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
    
    json.PrintObjectEnd();
}

void JSONConfigState::WriteShardServers()
{
    ConfigShardServer*   shardServer;
    
    json.PrintString(ReadBuffer("shardServers"));
    json.PrintColon();
    json.PrintArrayStart();

    for (shardServer = configState.shardServers.First(); shardServer != NULL; shardServer = configState.shardServers.Next(shardServer))
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
    for (it = list.First(); it != NULL; it = list.Next(it))
    {
        if (it != list.First())
            json.PrintComma();
        json.PrintNumber(*it);
    }
    json.PrintArrayEnd();
}
