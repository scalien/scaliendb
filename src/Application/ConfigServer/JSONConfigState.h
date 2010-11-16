#ifndef JSONCONFIGSTATE_H
#define JSONCONFIGSTATE_H

#include "Application/ConfigState/ConfigState.h"
#include "Application/HTTP/JSONSession.h"

class JSONConfigState
{
public:
    JSONConfigState(ConfigState& configState, JSONSession& json);
    
    void                Write();
    
private:
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

    template<typename List>
    void                WriteIDList(List& list);

    // this is not implemeted!
    JSONConfigState&    operator=(const JSONConfigState& other);

    ConfigState&        configState;
    JSONSession&        json;
};

#endif
