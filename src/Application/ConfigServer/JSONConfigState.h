#ifndef JSONCONFIGSTATE_H
#define JSONCONFIGSTATE_H

#include "Application/ConfigState/ConfigState.h"
#include "Application/ConfigServer/ConfigHeartbeatManager.h"
#include "Application/HTTP/JSONBufferWriter.h"

class ConfigServer;
class QuorumPaxosInfo;

/*
===============================================================================================

 JSONConfigState

===============================================================================================
*/

class JSONConfigState
{
public:
    JSONConfigState();

    void                SetHeartbeats(InSortedList<Heartbeat>* heartbeats);
    void                SetConfigState(ConfigState* configState);
    void                SetJSONBufferWriter(JSONBufferWriter* json);

    void                Write();
    
private:
    void                WriteControllers();
    void                WriteQuorums();
    void                WriteQuorum(ConfigQuorum* quorum);
    void                WriteDatabases();
    void                WriteDatabase(ConfigDatabase* database);
    void                WriteTables();
    void                WriteTable(ConfigTable* table);
    void                WriteShards();
    void                WriteShard(ConfigShard* shard);
    void                WriteShardServers();
    void                WriteShardServer(ConfigShardServer* server);
    void                WriteQuorumInfo(QuorumInfo* info);
    void                WriteQuorumShardInfo(QuorumShardInfo* info);

    template<typename List>
    void                WriteIDList(List& list);

    bool                HasHeartbeat(uint64_t nodeID);

    // this is not implemented!
    JSONConfigState&    operator=(const JSONConfigState& other);

    InSortedList<Heartbeat>*    heartbeats;
    ConfigState*                configState;
    JSONBufferWriter*           json;
};

#endif
