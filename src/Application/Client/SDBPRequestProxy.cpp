#include "SDBPRequestProxy.h"

using namespace SDBPClient;

static inline const Request* Key(const Request* req)
{
    return req;
}

static int KeyCmp(const Request* a, const Request* b)
{
    if (a->tableID < b->tableID)
        return -1;
    if (a->tableID > b->tableID)
        return 1;
    
    return Buffer::Cmp(a->key, b->key);
}

void RequestProxy::Init()
{
    size = 0;
}

Request* RequestProxy::RemoveAndAdd(Request* request)
{
    Request* it;

    it = Find(request);
    if (it)
        Remove(it);

    map.Insert<const Request*>(request);
    list.Append(request);
    ASSERT(map.GetCount() == list.GetLength());
    size += REQUEST_SIZE(request);
    return it;
}

Request* RequestProxy::Find(Request* query)
{
    return map.Get(query);
}

void RequestProxy::Remove(Request* request)
{
    size -= REQUEST_SIZE(request);
    ASSERT(size >= 0);
    ASSERT(map.GetCount() == list.GetLength());
    map.Remove(request);
    list.Remove(request);
    ASSERT(map.GetCount() == list.GetLength());
}

void RequestProxy::Clear()
{
    ASSERT(map.GetCount() == list.GetLength());
    ASSERT(size >= 0);
    map.Clear();
    list.Clear();
    size = 0;
}

unsigned RequestProxy::GetCount()
{
    ASSERT(map.GetCount() == list.GetLength());
    ASSERT(size >= 0);
    return list.GetLength();
}

unsigned RequestProxy::GetSize()
{
    ASSERT(size >= 0);
    return size;
}

Request* RequestProxy::Pop()
{
    Request* it;

    it = list.First();
    Remove(it);
    return it;
}

Request* RequestProxy::First()
{
    return map.First();
}

Request* RequestProxy::Last()
{
    return map.Last();
}

Request* RequestProxy::Next(Request* t)
{
    return map.Next(t);
}

Request* RequestProxy::Prev(Request* t)
{
    return map.Prev(t);
}
