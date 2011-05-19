#include "Test.h"
#include "Application/HTTP/JSONReader.h"

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

TEST_DEFINE(TestJSONAdvanced)
{

    const char*     configText = "{\"paxosID\":28,\"quorums\":[{\"quorumID\":2,\"hasPrimary\":\"true\",\"primaryID\":100,\"paxosID\":310,\"activeNodes\":[100,101],\"inactiveNodes\":[],\"shards\":[4]},{\"quorumID\":3,\"hasPrimary\":\"false\",\"paxosID\":186,\"activeNodes\":[102],\"inactiveNodes\":[103],\"shards\":[5]}],\"databases\":[{\"databaseID\":1,\"name\":\"testdb\",\"tables\":[1]}],\"tables\":[{\"tableID\":1,\"databaseID\":1,\"name\":\"testtable\",\"isFrozen\":false,\"shards\":[4,5]}],\"shards\":[{\"shardID\":4,\"quorumID\":2,\"tableID\":1,\"firstKey\":\"\",\"lastKey\":\"8\",\"shardSize\":238340427,\"splitKey\":\"3e3aa687770f55c704ca997c3be81634\",\"isSplitable\":false},{\"shardID\":5,\"quorumID\":3,\"tableID\":1,\"firstKey\":\"8\",\"lastKey\":\"\",\"shardSize\":237388662,\"splitKey\":\"bfd2308e9e75263970f8079115edebbd\",\"isSplitable\":false}],\"shardServers\":[{\"nodeID\":100,\"endpoint\":\"127.0.0.1:10010\",\"quorumInfos\":[{\"quorumID\":2,\"paxosID\":310,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":101,\"endpoint\":\"127.0.0.1:10020\",\"quorumInfos\":[{\"quorumID\":2,\"paxosID\":310,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":102,\"endpoint\":\"127.0.0.1:10030\",\"quorumInfos\":[{\"quorumID\":3,\"paxosID\":186,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]},{\"nodeID\":103,\"endpoint\":\"127.0.0.1:10040\",\"quorumInfos\":[{\"quorumID\":3,\"paxosID\":186,\"isSendingCatchup\":false}],\"quorumShardInfos\":[]}]}";
    ReadBuffer      jsonText(configText);
    JSON            root;
    JSONIterator    it;
    ReadBuffer      name;
    
    TEST_ASSERT(JSONReader::IsValid(jsonText));
    root = JSON::Create(jsonText);
    
    FOREACH (it, root)
    {
        TEST_ASSERT(it.IsPair());
        TEST_ASSERT(it.GetName(name));
        TEST_LOG("%.*s", name.GetLength(), name.GetBuffer());
    }
    
    return TEST_SUCCESS;
}