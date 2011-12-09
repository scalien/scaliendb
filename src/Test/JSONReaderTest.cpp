#include "Test.h"
#include "Application/HTTP/JSONReader.h"
#include "Application/ConfigState/ConfigState.h"

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
    const char*     configText = "{\"paxosID\":28,\"quorums\":[{\"quorumID\":2,\"hasPrimary\":true,\"primaryID\":100,\"paxosID\":310,\"activeNodes\":[100,101],\"inactiveNodes\":[],\"shards\":[4]},{\"quorumID\":3,\"hasPrimary\":\"false\",\"paxosID\":186,\"activeNodes\":[102],\"inactiveNodes\":[103],\"shards\":[5]}],\"databases\":[{\"databaseID\":1,\"name\":\"testdb\",\"tables\":[1]}],\"tables\":[{\"tableID\":1,\"databaseID\":1,\"name\":\"testtable\",\"isFrozen\":false,\"shards\":[4,5]}],\"shards\":[{\"shardID\":4,\"quorumID\":2,\"tableID\":1,\"firstKey\":\"\",\"lastKey\":\"8\",\"shardSize\":238340427,\"splitKey\":\"3e3aa687770f55c704ca997c3be81634\",\"isSplitable\":false},{\"shardID\":5,\"quorumID\":3,\"tableID\":1,\"firstKey\":\"8\",\"lastKey\":\"\",\"shardSize\":237388662,\"splitKey\":\"bfd2308e9e75263970f8079115edebbd\",\"isSplitable\":false}],\"shardServers\":[{\"nodeID\":100,\"endpoint\":\"127.0.0.1:10010\",\"quorumInfos\":[{\"quorumID\":2,\"paxosID\":310,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":101,\"endpoint\":\"127.0.0.1:10020\",\"quorumInfos\":[{\"quorumID\":2,\"paxosID\":310,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":102,\"endpoint\":\"127.0.0.1:10030\",\"quorumInfos\":[{\"quorumID\":3,\"paxosID\":186,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":103,\"endpoint\":\"127.0.0.1:10040\",\"quorumInfos\":[{\"quorumID\":3,\"paxosID\":186,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]}]}";
    ReadBuffer      jsonText(configText);
    JSON            root;
    JSON            value;
    JSONIterator    it;
    JSONIterator    itValue;
    ReadBuffer      name;
    ConfigState     configState;
    
    TEST_ASSERT(JSONReader::IsValid(jsonText));
    root = JSON::Create(jsonText);
    
    JSONConfigStateReader_Read(root, configState);
    
    return TEST_SUCCESS;
}

