#include "Test.h"
#include "System/Stopwatch.h"
#include "Application/HTTP/JSONReader.h"
#include "Application/HTTP/JSONBufferWriter.h"
#include "Application/ConfigState/ConfigState.h"
#include "Application/ConfigServer/JSONConfigState.h"

TEST_DEFINE(TestJSONReaderBasic1)
{
    ReadBuffer  json("{\"key1\":\"value1\"}");
    ReadBuffer  value;
    ReadBuffer  stringValue;
    
    TEST_ASSERT(JSONReader::IsValid(json));
    
    if (!JSONReader::FindValue(json, "key1", value))
        return TEST_FAILURE;
    
    if (!JSONReader::GetStringValue(value, stringValue))
        return TEST_FAILURE;
    
    TEST_ASSERT(ReadBuffer::Cmp(stringValue, "value1") == 0);
    TEST_LOG("%.*s", stringValue.GetLength(), stringValue.GetBuffer());
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestJSONReaderBasic2)
{
    ReadBuffer  json("{\"key1\":1}");
    ReadBuffer  member;
    int64_t     value;

    TEST_ASSERT(JSONReader::IsValid(json));
    
    if (!JSONReader::FindMember(json, "key1", member))
        return TEST_FAILURE;
    
    if (!JSONReader::MemberInt64Value(member, value))
        return TEST_FAILURE;
    
    TEST_ASSERT(value == 1);
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestJSONReaderBasic3)
{
    ReadBuffer  json("{\"key1\":{\"key1_1\":\"value1_1\"}}");
    ReadBuffer  member;
    ReadBuffer  value;

    TEST_ASSERT(JSONReader::IsValid(json));
    
    if (!JSONReader::FindMember(json, "key1.key1_1", member))
        return TEST_FAILURE;
    
    if (!JSONReader::MemberStringValue(member, value))
        return TEST_FAILURE;
    
    TEST_ASSERT(ReadBuffer::Cmp(value, "value1_1") == 0);
    TEST_LOG("%.*s", value.GetLength(), value.GetBuffer());
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestJSONReaderBasic4)
{
    ReadBuffer  json("{\"key1\":{\"key1_1\":\"value1_1\"}}");
    ReadBuffer  member;
    ReadBuffer  value;

    TEST_ASSERT(JSONReader::IsValid(json));
    
    if (!JSONReader::FindMember(json, "key1[\"key1_1\"]", member))
        return TEST_FAILURE;
    
    if (!JSONReader::MemberStringValue(member, value))
        return TEST_FAILURE;
    
    TEST_ASSERT(ReadBuffer::Cmp(value, "value1_1") == 0);
    TEST_LOG("%.*s", value.GetLength(), value.GetBuffer());
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestJSONReaderBasic5)
{
    ReadBuffer  json("{\"key1\":[\"value1_1\",\"value1_2\"]}");
    ReadBuffer  value;
    ReadBuffer  stringValue;

    TEST_ASSERT(JSONReader::IsValid(json));
    
    if (!JSONReader::FindValue(json, "key1[1]", value))
        return TEST_FAILURE;
        
    if (!JSONReader::GetStringValue(value, stringValue))
        return TEST_FAILURE;
    
    TEST_ASSERT(ReadBuffer::Cmp(stringValue, "value1_2") == 0);
    TEST_LOG("%.*s", stringValue.GetLength(), stringValue.GetBuffer());
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestJSONReaderBasic6)
{
    ReadBuffer  json("{\"key1\":[\"value1_1\",\"value1_2\"]}");
    ReadBuffer  key1;
    ReadBuffer  value;
    unsigned    length;

    TEST_ASSERT(JSONReader::IsValid(json));
    
    if (!JSONReader::FindValue(json, "key1", value))
        return TEST_FAILURE;
        
    if (!JSONReader::GetArray(value, key1, length))
        return TEST_FAILURE;
    
    TEST_LOG("array length: %d, raw: %.*s", length, key1.GetLength(), key1.GetBuffer());
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestJSONReaderBasic7)
{
    ReadBuffer  json("{\"key1\":[\"value1_1\",\"value1_2\"]}");
    ReadBuffer  key1;
    ReadBuffer  value;
    ReadBuffer  value1_2;
    ReadBuffer  stringValue;
    unsigned    length;

    TEST_ASSERT(JSONReader::IsValid(json));
    
    if (!JSONReader::FindValue(json, "key1", value))
        return TEST_FAILURE;
        
    if (!JSONReader::GetArray(value, key1, length))
        return TEST_FAILURE;
        
    TEST_ASSERT(length == 2);
    
    if (!JSONReader::GetArrayElement(value, 1, value1_2))
        return TEST_FAILURE;
        
    if (!JSONReader::GetStringValue(value1_2, stringValue))
        return TEST_FAILURE;
    
    TEST_ASSERT(ReadBuffer::Cmp(stringValue, "value1_2") == 0);
    TEST_LOG("%.*s", stringValue.GetLength(), stringValue.GetBuffer());
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestJSONBasic)
{
    ReadBuffer      json("{\"key1\":[\"value1_1\",\"value1_2\"]}");
    JSON            root;
    JSON            key1;
    JSON            value;
    JSONIterator    it;
    ReadBuffer      stringValue;

    TEST_ASSERT(JSONReader::IsValid(json));

    root = JSON::Create(json);
    TEST_ASSERT(root.IsObject());
    TEST_ASSERT(root.GetMember("key1", key1));
    TEST_ASSERT(key1.IsArray());
        
    FOREACH (it, key1)
    {
        TEST_ASSERT(!it.IsPair());
        TEST_ASSERT(it.GetValue(value));
        TEST_ASSERT(value.GetStringValue(stringValue));
        TEST_LOG("%.*s", stringValue.GetLength(), stringValue.GetBuffer());
    }
    
    return TEST_SUCCESS;
}

static void TestJSONWriter(JSON json)
{
    JSONIterator    it;
    JSON            value;
    ReadBuffer      name;
    ReadBuffer      buffer;

    if (json.IsArray())
    {
        FOREACH (it, json)
        {
            if (it.GetValue(value))
            {
                TestJSONWriter(value);
            }
        }
    }
    else if (json.IsObject())
    {
        FOREACH (it, json)
        {
            TEST_ASSERT(it.GetName(name));
            TEST_LOG("%.*s", name.GetLength(), name.GetBuffer());
            if (it.GetValue(value))
            {
                TestJSONWriter(value);
            }
        }
    }
    else
    {
        json.GetStringValue(buffer);
        TEST_LOG("%.*s", buffer.GetLength(), buffer.GetBuffer());
    }
}

typedef bool (*JSONIterator_Setter)(JSONIterator it, void* ptr);

struct JSONReaderRule
{
    ReadBuffer          name;
    JSONIterator_Setter setter;
    void*               ptr;
};

static bool JSONIterator_SetUInt(JSONIterator it, void* ptr)
{
    JSON    value;
    int64_t intValue;

    if (!it.GetValue(value))
        return false;
    
    if (!value.GetInt64Value(intValue))
        return false;

    *(unsigned int*) ptr = (unsigned int) intValue;

    return true;
}

static bool JSONIterator_SetUInt64(JSONIterator it, void* ptr)
{
    JSON    value;

    if (!it.GetValue(value))
        return false;
    
    if (!value.GetInt64Value(*(int64_t*) ptr))
        return false;

    return true;
}

static bool JSONIterator_SetBool(JSONIterator it, void* ptr)
{
    JSON    value;

    if (!it.GetValue(value))
        return false;
    
    if (!value.GetBoolValue(*(bool*) ptr))
        return false;

    return true;
}

static bool JSONIterator_SetBuffer(JSONIterator it, void* ptr)
{
    JSON        value;
    ReadBuffer  stringValue;
    Buffer*     buffer;

    if (!it.GetValue(value))
        return false;

    if (!value.GetStringValue(stringValue))
        return false;

    buffer = (Buffer*) ptr;
    buffer->Write(stringValue);

    return true;
}

static bool JSONIterator_SetListUInt64(JSONIterator it, void* ptr)
{
    JSON                    jsonArray;
    JSONIterator            itArray;
    JSON                    value;
    uint64_t                item;
    List<uint64_t>*         list;

    list = (List<uint64_t>*) ptr;

    if (!it.GetValue(jsonArray) || !jsonArray.IsArray())
        return false;

    FOREACH (itArray, jsonArray)
    {
        if (!itArray.GetValue(value))
            return false;

        if (!value.GetInt64Value((int64_t&) item))
            return false;

        list->Add(item);
    }

    return true;
}

static bool LessThan(uint64_t a, uint64_t b)
{
    return a < b;
}

static bool JSONIterator_SetSortedListUInt64(JSONIterator it, void* ptr)
{
    JSON                    jsonArray;
    JSONIterator            itArray;
    JSON                    value;
    uint64_t                item;
    SortedList<uint64_t>*   list;

    list = (SortedList<uint64_t>*) ptr;

    if (!it.GetValue(jsonArray) || !jsonArray.IsArray())
        return false;

    FOREACH (itArray, jsonArray)
    {
        if (!itArray.GetValue(value))
            return false;

        if (!value.GetInt64Value((int64_t&) item))
            return false;

        list->Add(item);
    }

    return true;
}

static bool JSONIterator_SetEndpoint(JSONIterator it, void* ptr)
{
    JSON        value;
    ReadBuffer  stringValue;
    Endpoint*   endpoint;

    if (!it.GetValue(value))
        return false;

    if (!value.GetStringValue(stringValue))
        return false;

    endpoint = (Endpoint*) ptr;
    endpoint->Set(stringValue);

    return true;
}

static bool JSONConfigStateReader_ReadByRules(JSON json, JSONReaderRule* rules, size_t numRules)
{
    JSONIterator    it;
    ReadBuffer      name;
    size_t          index;
    size_t          oldIndex;

    index = 0;
    oldIndex = numRules;
    FOREACH (it, json)
    {
        if (!it.GetName(name))
            return false;

        // Optimization: if the ordering of the rules follows the ordering of the generated JSON members,
        // then we don't have to make all the comparisons in the ruleset, because we remember the last index,
        // and start the comparison from there.
        
        for (; index != oldIndex; index = (index + 1) % numRules)
        {
            if (ReadBuffer::Cmp(name, rules[index].name) == 0)
            {
                if (!rules[index].setter(it, rules[index].ptr))
                    return false;
                break;
            }
            
            if (oldIndex == numRules)
                oldIndex = 0;
        }
        oldIndex = index;
        index = (index + 1) % numRules;
    }

    return true;
}

static bool JSONConfigStateReader_ReadQuorum(JSON json, ConfigQuorum* quorum)
{
    JSONReaderRule  rules[] = {
                                {"quorumID", JSONIterator_SetUInt64, &quorum->quorumID},
                                {"hasPrimary", JSONIterator_SetBool, &quorum->hasPrimary},
                                {"primaryID", JSONIterator_SetUInt64, &quorum->primaryID},
                                {"paxosID", JSONIterator_SetUInt64, &quorum->paxosID},
                                {"activeNodes", JSONIterator_SetSortedListUInt64, &quorum->activeNodes},
                                {"inactiveNodes", JSONIterator_SetSortedListUInt64, &quorum->inactiveNodes},
                                {"shards", JSONIterator_SetSortedListUInt64, &quorum->shards}
                            };


    return JSONConfigStateReader_ReadByRules(json, rules, SIZE(rules));
}

static bool JSONConfigStateReader_ReadQuorums(JSON json, ConfigState& configState)
{
    JSONIterator    it;
    JSON            value;
    ConfigQuorum*   quorum;

    FOREACH (it, json)
    {
        if (!it.GetValue(value))
            return false;
        quorum = new ConfigQuorum;
        configState.quorums.Append(quorum);
        if (!JSONConfigStateReader_ReadQuorum(value, quorum))
            return false;
    }

    return true;
}

static bool JSONConfigStateReader_ReadDatabase(JSON json, ConfigDatabase* database)
{
    JSONReaderRule  rules[] = {
                                {"databaseID", JSONIterator_SetUInt64, &database->databaseID},
                                {"name", JSONIterator_SetBuffer, &database->name},
                                {"tables", JSONIterator_SetListUInt64, &database->tables},
                            };


    return JSONConfigStateReader_ReadByRules(json, rules, SIZE(rules));
}

static bool JSONConfigStateReader_ReadDatabases(JSON json, ConfigState& configState)
{
    JSONIterator        it;
    JSON                value;
    ConfigDatabase*     database;

    FOREACH (it, json)
    {
        if (!it.GetValue(value))
            return false;
        database = new ConfigDatabase;
        configState.databases.Append(database);
        if (!JSONConfigStateReader_ReadDatabase(value, database))
            return false;
    }

    return true;
}

static bool JSONConfigStateReader_ReadTable(JSON json, ConfigTable* table)
{
    JSONReaderRule  rules[] = {
                                {"tableID", JSONIterator_SetUInt64, &table->tableID},
                                {"databaseID", JSONIterator_SetUInt64, &table->databaseID},
                                {"name", JSONIterator_SetBuffer, &table->name},
                                {"isFrozen", JSONIterator_SetBool, &table->isFrozen},
                                {"shards", JSONIterator_SetListUInt64, &table->shards},
                            };


    return JSONConfigStateReader_ReadByRules(json, rules, SIZE(rules));
}

static bool JSONConfigStateReader_ReadTables(JSON json, ConfigState& configState)
{
    JSONIterator    it;
    JSON            value;
    ConfigTable*    table;

    FOREACH (it, json)
    {
        if (!it.GetValue(value))
            return false;
        table = new ConfigTable;
        configState.tables.Append(table);
        if (!JSONConfigStateReader_ReadTable(value, table))
            return false;
    }

    return true;
}

static bool JSONConfigStateReader_ReadShard(JSON json, ConfigShard* shard)
{
    JSONReaderRule  rules[] = {
                                {"shardID", JSONIterator_SetUInt64, &shard->shardID},
                                {"quorumID", JSONIterator_SetUInt64, &shard->quorumID},
                                {"tableID", JSONIterator_SetUInt64, &shard->tableID},
                                {"firstKey", JSONIterator_SetBuffer, &shard->firstKey},
                                {"lastKey", JSONIterator_SetBuffer, &shard->lastKey},
                                {"shardSize", JSONIterator_SetUInt64, &shard->shardSize},
                                {"splitKey", JSONIterator_SetBuffer, &shard->splitKey},
                                {"isSplitable", JSONIterator_SetBool, &shard->isSplitable},
                            };


    return JSONConfigStateReader_ReadByRules(json, rules, SIZE(rules));
}

static bool JSONConfigStateReader_ReadShards(JSON json, ConfigState& configState)
{
    JSONIterator    it;
    JSON            value;
    ConfigShard*    shard;

    FOREACH (it, json)
    {
        if (!it.GetValue(value))
            return false;
        shard = new ConfigShard;
        configState.shards.Append(shard);
        if (!JSONConfigStateReader_ReadShard(value, shard))
            return false;
    }

    return true;
}

static bool JSONConfigStateReader_ReadShardServer(JSON json, ConfigShardServer* shardServer)
{
    JSONReaderRule  rules[] = {
                                {"nodeID", JSONIterator_SetUInt64, &shardServer->nodeID},
                                {"endpoint", JSONIterator_SetEndpoint, &shardServer->endpoint},
                                {"httpPort", JSONIterator_SetUInt, &shardServer->httpPort},
                                {"sdbpPort", JSONIterator_SetUInt, &shardServer->sdbpPort},
                                {"hasHeartbeat", JSONIterator_SetBool, &shardServer->hasHeartbeat},
                                //{"quorumInfos", JSONIterator_SetQuorumInfos, &shardServer->quorumInfos},
                                //{"quorumShardInfos", JSONIterator_SetQuorumShardInfos, shardServer},
                                //{"quorumPriorites", JSONIterator_SetQuorumPriorities, shardServer},
                            };


    return JSONConfigStateReader_ReadByRules(json, rules, SIZE(rules));
}

static bool JSONConfigStateReader_ReadShardServers(JSON json, ConfigState& configState)
{
    JSONIterator        it;
    JSON                value;
    ConfigShardServer*  shardServer;

    FOREACH (it, json)
    {
        if (!it.GetValue(value))
            return false;
        shardServer = new ConfigShardServer;
        configState.shardServers.Append(shardServer);
        if (!JSONConfigStateReader_ReadShardServer(value, shardServer))
            return false;
    }

    return true;
}

static bool JSONConfigStateReader_Read(JSON json, ConfigState& configState)
{
    JSONIterator    it;
    ReadBuffer      name;
    JSON            value;

    FOREACH (it, json)
    {
        if (!it.GetName(name) || !it.GetValue(value))
            return false;
        
        if (name.Equals("paxosID"))
        {
            if (!value.GetInt64Value((int64_t&)configState.paxosID))
                return false;
        }
        else if (name.Equals("quorums"))
        {
            if (!JSONConfigStateReader_ReadQuorums(value, configState))
                return false;
        }
        else if (name.Equals("databases"))
        {
            if (!JSONConfigStateReader_ReadDatabases(value, configState))
                return false;
        }
        else if (name.Equals("tables"))
        {
            if (!JSONConfigStateReader_ReadTables(value, configState))
                return false;
        }
        else if (name.Equals("shards"))
        {
            if (!JSONConfigStateReader_ReadShards(value, configState))
                return false;
        }
        else if (name.Equals("shardServers"))
        {
            if (!JSONConfigStateReader_ReadShardServers(value, configState))
                return false;
        }
    }

    return true;
}

TEST_DEFINE(TestJSONAdvanced)
{
    const char*     configText = "{\"paxosID\":28,\"quorums\":[{\"quorumID\":2,\"hasPrimary\":true,\"primaryID\":100,\"paxosID\":310,\"activeNodes\":[100,101],\"inactiveNodes\":[],\"shards\":[4]},{\"quorumID\":3,\"hasPrimary\":\"false\",\"paxosID\":186,\"activeNodes\":[102],\"inactiveNodes\":[103],\"shards\":[5]}],\"databases\":[{\"databaseID\":1,\"name\":\"testdb\",\"tables\":[1]}],\"tables\":[{\"tableID\":1,\"databaseID\":1,\"name\":\"testtable\",\"isFrozen\":false,\"shards\":[4,5]}],\"shards\":[{\"shardID\":4,\"quorumID\":2,\"tableID\":1,\"firstKey\":\"\",\"lastKey\":\"8\",\"shardSize\":238340427,\"splitKey\":\"3e3aa687770f55c704ca997c3be81634\",\"isSplitable\":false},{\"shardID\":5,\"quorumID\":3,\"tableID\":1,\"firstKey\":\"8\",\"lastKey\":\"\",\"shardSize\":237388662,\"splitKey\":\"bfd2308e9e75263970f8079115edebbd\",\"isSplitable\":false}],\"shardServers\":[{\"nodeID\":100,\"endpoint\":\"127.0.0.1:10010\",\"quorumInfos\":[{\"quorumID\":2,\"paxosID\":310,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":101,\"endpoint\":\"127.0.0.1:10020\",\"quorumInfos\":[{\"quorumID\":2,\"paxosID\":310,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":102,\"endpoint\":\"127.0.0.1:10030\",\"quorumInfos\":[{\"quorumID\":3,\"paxosID\":186,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":103,\"endpoint\":\"127.0.0.1:10040\",\"quorumInfos\":[{\"quorumID\":3,\"paxosID\":186,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]}]}";
    ReadBuffer      jsonText(configText);
    JSON            root;
    JSON            value;
    JSONIterator    it;
    JSONIterator    itValue;
    ReadBuffer      name;
    
    TEST_ASSERT(JSONReader::IsValid(jsonText));
    root = JSON::Create(jsonText);
    
    TestJSONWriter(root);
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestJSONConfigStateReader)
{
    const char*     configText = "{\"paxosID\":28,\"quorums\":[{\"quorumID\":2,\"hasPrimary\":true,\"primaryID\":100,\"paxosID\":310,\"activeNodes\":[100,101],\"inactiveNodes\":[],\"shards\":[4]},{\"quorumID\":3,\"hasPrimary\":false,\"paxosID\":186,\"activeNodes\":[102],\"inactiveNodes\":[103],\"shards\":[5]}],\"databases\":[{\"databaseID\":1,\"name\":\"testdb\",\"tables\":[1]}],\"tables\":[{\"tableID\":1,\"databaseID\":1,\"name\":\"testtable\",\"isFrozen\":false,\"shards\":[4,5]}],\"shards\":[{\"shardID\":4,\"quorumID\":2,\"tableID\":1,\"firstKey\":\"\",\"lastKey\":\"8\",\"shardSize\":238340427,\"splitKey\":\"3e3aa687770f55c704ca997c3be81634\",\"isSplitable\":false},{\"shardID\":5,\"quorumID\":3,\"tableID\":1,\"firstKey\":\"8\",\"lastKey\":\"\",\"shardSize\":237388662,\"splitKey\":\"bfd2308e9e75263970f8079115edebbd\",\"isSplitable\":false}],\"shardServers\":[{\"nodeID\":100,\"endpoint\":\"127.0.0.1:10010\",\"quorumInfos\":[{\"quorumID\":2,\"paxosID\":310,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":101,\"endpoint\":\"127.0.0.1:10020\",\"quorumInfos\":[{\"quorumID\":2,\"paxosID\":310,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":102,\"endpoint\":\"127.0.0.1:10030\",\"quorumInfos\":[{\"quorumID\":3,\"paxosID\":186,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":103,\"endpoint\":\"127.0.0.1:10040\",\"quorumInfos\":[{\"quorumID\":3,\"paxosID\":186,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]}]}";
    ReadBuffer      jsonText(configText);
    JSON            root;
    ConfigState     configState;
    
    TEST_ASSERT(JSONReader::IsValid(jsonText));
    root = JSON::Create(jsonText);
    
    TEST_ASSERT(JSONConfigStateReader_Read(root, configState));
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestJSONConfigStateReader_ReadAndWrite)
{
    const char*         configText = "{\"paxosID\":28,\"quorums\":[{\"quorumID\":2,\"hasPrimary\":true,\"primaryID\":100,\"paxosID\":310,\"activeNodes\":[100,101],\"inactiveNodes\":[],\"shards\":[4]},{\"quorumID\":3,\"hasPrimary\":false,\"paxosID\":186,\"activeNodes\":[102],\"inactiveNodes\":[103],\"shards\":[5]}],\"databases\":[{\"databaseID\":1,\"name\":\"testdb\",\"tables\":[1]}],\"tables\":[{\"tableID\":1,\"databaseID\":1,\"name\":\"testtable\",\"isFrozen\":false,\"shards\":[4,5]}],\"shards\":[{\"shardID\":4,\"quorumID\":2,\"tableID\":1,\"firstKey\":\"\",\"lastKey\":\"8\",\"shardSize\":238340427,\"splitKey\":\"3e3aa687770f55c704ca997c3be81634\",\"isSplitable\":false},{\"shardID\":5,\"quorumID\":3,\"tableID\":1,\"firstKey\":\"8\",\"lastKey\":\"\",\"shardSize\":237388662,\"splitKey\":\"bfd2308e9e75263970f8079115edebbd\",\"isSplitable\":false}],\"shardServers\":[{\"nodeID\":100,\"endpoint\":\"127.0.0.1:10010\",\"quorumInfos\":[{\"quorumID\":2,\"paxosID\":310,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":101,\"endpoint\":\"127.0.0.1:10020\",\"quorumInfos\":[{\"quorumID\":2,\"paxosID\":310,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":102,\"endpoint\":\"127.0.0.1:10030\",\"quorumInfos\":[{\"quorumID\":3,\"paxosID\":186,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":103,\"endpoint\":\"127.0.0.1:10040\",\"quorumInfos\":[{\"quorumID\":3,\"paxosID\":186,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]}]}";
    ReadBuffer          jsonText(configText);
    JSON                root;
    ConfigState         configState;
    ConfigState         configState2;
    JSONBufferWriter    writer;
    JSONConfigState     jsonConfigState;
    Buffer              output;
    Buffer              output2;

    TEST_ASSERT(JSONReader::IsValid(jsonText));
    root = JSON::Create(jsonText);
    
    TEST_ASSERT(JSONConfigStateReader_Read(root, configState));

    writer.Init(&output);
    
    writer.Start();
    jsonConfigState.SetJSONBufferWriter(&writer);
    jsonConfigState.SetConfigState(&configState);
    jsonConfigState.Write();
    writer.End();

    jsonText.Wrap(output);

    TEST_ASSERT(JSONReader::IsValid(jsonText));
    root = JSON::Create(jsonText);
    
    TEST_ASSERT(JSONConfigStateReader_Read(root, configState2));

    writer.Init(&output2);
    
    writer.Start();
    jsonConfigState.SetJSONBufferWriter(&writer);
    jsonConfigState.SetConfigState(&configState2);
    jsonConfigState.Write();
    writer.End();

    TEST_ASSERT(Buffer::Cmp(output, output2) == 0);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestJSONConfigStateReader_Benchmark)
{
    const char*         configText = "{\"paxosID\":28,\"quorums\":[{\"quorumID\":2,\"hasPrimary\":true,\"primaryID\":100,\"paxosID\":310,\"activeNodes\":[100,101],\"inactiveNodes\":[],\"shards\":[4]},{\"quorumID\":3,\"hasPrimary\":false,\"paxosID\":186,\"activeNodes\":[102],\"inactiveNodes\":[103],\"shards\":[5]}],\"databases\":[{\"databaseID\":1,\"name\":\"testdb\",\"tables\":[1]}],\"tables\":[{\"tableID\":1,\"databaseID\":1,\"name\":\"testtable\",\"isFrozen\":false,\"shards\":[4,5]}],\"shards\":[{\"shardID\":4,\"quorumID\":2,\"tableID\":1,\"firstKey\":\"\",\"lastKey\":\"8\",\"shardSize\":238340427,\"splitKey\":\"3e3aa687770f55c704ca997c3be81634\",\"isSplitable\":false},{\"shardID\":5,\"quorumID\":3,\"tableID\":1,\"firstKey\":\"8\",\"lastKey\":\"\",\"shardSize\":237388662,\"splitKey\":\"bfd2308e9e75263970f8079115edebbd\",\"isSplitable\":false}],\"shardServers\":[{\"nodeID\":100,\"endpoint\":\"127.0.0.1:10010\",\"quorumInfos\":[{\"quorumID\":2,\"paxosID\":310,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":101,\"endpoint\":\"127.0.0.1:10020\",\"quorumInfos\":[{\"quorumID\":2,\"paxosID\":310,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":102,\"endpoint\":\"127.0.0.1:10030\",\"quorumInfos\":[{\"quorumID\":3,\"paxosID\":186,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":103,\"endpoint\":\"127.0.0.1:10040\",\"quorumInfos\":[{\"quorumID\":3,\"paxosID\":186,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]}]}";
    ReadBuffer          jsonText(configText);
    JSON                root;
    ConfigState         referenceConfigState;
    ConfigState         configState;
    Stopwatch           writerSw;
    Stopwatch           readerSw;
    Buffer              output;
    Buffer              output2;
    int                 i;
    JSONBufferWriter    writer;
    JSONConfigState     jsonConfigState;
    unsigned            numBytes;
    ReadBuffer          rb;

    TEST_ASSERT(JSONReader::IsValid(jsonText));
    root = JSON::Create(jsonText);
    
    TEST_ASSERT(JSONConfigStateReader_Read(root, referenceConfigState));
    
    //numBytes = 0;
    //writerSw.Reset();
    //readerSw.Reset();
    //for (i = 0; i < 1000*1000; i++)
    //{
    //    output.Clear();
    //    writerSw.Start();
    //    referenceConfigState.Write(output, true);
    //    writerSw.Stop();
    //    numBytes += output.GetLength();

    //    rb.Wrap(output);
    //    readerSw.Start();
    //    TEST_ASSERT(configState.Read(rb, true));
    //    readerSw.Stop();

    //    TEST_ASSERT(configState.controllers.GetLength() == referenceConfigState.controllers.GetLength());
    //    TEST_ASSERT(configState.quorums.GetLength() == referenceConfigState.quorums.GetLength());
    //    TEST_ASSERT(configState.databases.GetLength() == referenceConfigState.databases.GetLength());
    //    TEST_ASSERT(configState.tables.GetLength() == referenceConfigState.tables.GetLength());
    //    TEST_ASSERT(configState.shards.GetLength() == referenceConfigState.shards.GetLength());
    //    TEST_ASSERT(configState.shardServers.GetLength() == referenceConfigState.shardServers.GetLength());
    //}
    //writerSw.Stop();
    //TEST_LOG("ConfigState writer elapsed: %u, bytes: %u, bps: %s", writerSw.Elapsed(), numBytes, HUMAN_BYTES(numBytes * 1000.0 / writerSw.Elapsed()));
    //TEST_LOG("ConfigState reader elapsed: %u", readerSw.Elapsed());

    writer.Init(&output);
    jsonConfigState.SetJSONBufferWriter(&writer);
    jsonConfigState.SetConfigState(&referenceConfigState);
    
    numBytes = 0;
    writerSw.Reset();
    readerSw.Reset();
    for (i = 0; i < 100*1000; i++)
    {
        output.Clear();
        writer.Init(&output);
        writer.Start();
        writerSw.Start();
        jsonConfigState.Write();
        writerSw.Stop();
        writer.End();
        numBytes += output.GetLength();

        configState.Init();
        rb.Wrap(output);
        readerSw.Start();
        TEST_ASSERT(JSONConfigStateReader_Read(JSON::Create(rb), configState));
        readerSw.Stop();

        TEST_ASSERT(configState.controllers.GetLength() == referenceConfigState.controllers.GetLength());
        TEST_ASSERT(configState.quorums.GetLength() == referenceConfigState.quorums.GetLength());
        TEST_ASSERT(configState.databases.GetLength() == referenceConfigState.databases.GetLength());
        TEST_ASSERT(configState.tables.GetLength() == referenceConfigState.tables.GetLength());
        TEST_ASSERT(configState.shards.GetLength() == referenceConfigState.shards.GetLength());
        TEST_ASSERT(configState.shardServers.GetLength() == referenceConfigState.shardServers.GetLength());
    }
    TEST_LOG("JSONConfigState writer elapsed: %u, bytes: %u, bps: %s", writerSw.Elapsed(), numBytes, HUMAN_BYTES(numBytes * 1000.0 / writerSw.Elapsed()));
    TEST_LOG("JSONConfigState reader elapsed: %u", readerSw.Elapsed());

    return TEST_SUCCESS;
}
