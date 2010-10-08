#include "QuorumDatabase.h"
#include "Framework/Storage/StorageEnvironment.h"

void QuorumDatabase::Init(StorageTable* table_, uint64_t logCacheSize_)
{
    assert(table_ != NULL);
    table = table_;
    logCacheSize = logCacheSize_;
}

uint64_t QuorumDatabase::GetPaxosID()
{
    return GetUint64("paxosID");
}

void QuorumDatabase::SetPaxosID(uint64_t paxosID)
{
    SetUint64("paxosID", paxosID);
}

bool QuorumDatabase::GetAccepted()
{
    Buffer      key;
    ReadBuffer  rbKey;
    ReadBuffer  rbValue;
    bool        ret;

    key.Write("accepted");
    rbKey.Wrap(key);

    ret = table->Get(rbKey, rbValue);
    if (!ret)
      return false;   // not found, return default

    if (rbValue.GetLength() != 1)
      return false;   // incorrect value, return default
      
    if (rbValue.GetCharAt(0) == '1')
      return true;

    return false;
}

void QuorumDatabase::SetAccepted(bool accepted)
{
    Buffer      key;
    Buffer      value;
    ReadBuffer  rbKey;
    ReadBuffer  rbValue;
    bool        ret;

    key.Write("accepted");
    rbKey.Wrap(key);

    value.Writef("%d", accepted);
    rbValue.Wrap(value);
    
    ret = table->Set(rbKey, rbValue);
}

uint64_t QuorumDatabase::GetPromisedProposalID()
{
    return GetUint64("promisedProposalID");
}

void QuorumDatabase::SetPromisedProposalID(uint64_t promisedProposalID)
{
    SetUint64("promisedProposalID", promisedProposalID);
}

uint64_t QuorumDatabase::GetAcceptedProposalID()
{
    return GetUint64("acceptedProposalID");
}

void QuorumDatabase::SetAcceptedProposalID(uint64_t acceptedProposalID)
{
    SetUint64("acceptedProposalID", acceptedProposalID);
}

uint64_t QuorumDatabase::GetAcceptedRunID()
{
    return GetUint64("acceptedRunID");
}

void QuorumDatabase::SetAcceptedRunID(uint64_t acceptedRunID)
{
    SetUint64("acceptedRunID", acceptedRunID);
}

void QuorumDatabase::GetAcceptedValue(Buffer& acceptedValue)
{
    Buffer      key;
    ReadBuffer  rbKey;
    ReadBuffer  rbValue;
    bool        ret;

    key.Write("acceptedValue");
    rbKey.Wrap(key);

    ret = table->Get(rbKey, rbValue);
    if (!ret)
        return;
    
    acceptedValue.Write(rbValue.GetBuffer(), rbValue.GetLength());
}

void QuorumDatabase::SetAcceptedValue(Buffer& acceptedValue)
{
    Buffer      key;
    ReadBuffer  rbKey;
    ReadBuffer  rbValue;
    bool        ret;

    key.Write("acceptedValue");
    rbKey.Wrap(key);
    rbValue.Wrap(acceptedValue);

    ret = table->Set(rbKey, rbValue);
}

void QuorumDatabase::GetLearnedValue(uint64_t paxosID, Buffer& value)
{
    Buffer      key;
    ReadBuffer  rbKey;
    ReadBuffer  rbValue;
    bool        ret;

    key.Writef("learnedValue:%U", paxosID);
    rbKey.Wrap(key);
    
    ret = table->Get(rbKey, rbValue);
    if (!ret)
        return;
    
    value.Write(rbValue.GetBuffer(), rbValue.GetLength());
}

void QuorumDatabase::SetLearnedValue(uint64_t paxosID, ReadBuffer& value)
{
    Buffer      key;
    ReadBuffer  rbKey;

    key.Writef("learnedValue:%U", paxosID);
    rbKey.Wrap(key);

    table->Set(rbKey, value);
}

bool QuorumDatabase::IsActive()
{
    // TODO: for async write
    return false;
}

void QuorumDatabase::Commit()
{
    table->GetEnvironment()->Commit();
}

uint64_t QuorumDatabase::GetUint64(const char* name)
{
    ReadBuffer  key(name);
    ReadBuffer  value;
    uint64_t    u64;
    unsigned    nread;
    int         ret;

    ret = table->Get(key, value);
    if (!ret)
        return false;

    nread = 0;
    u64 = BufferToUInt64(value.GetBuffer(), value.GetLength(), &nread);
    if (nread != value.GetLength())
    {
        Log_Trace();
        u64 = 0;
    }

    return u64;
}

void QuorumDatabase::SetUint64(const char* name, uint64_t u64)
{
    ReadBuffer  key(name);
    Buffer      tmp;
    ReadBuffer  value;
    int         ret;

    tmp.Writef("%U", u64);
    value.Wrap(tmp);

    ret = table->Set(key, value);
}

void QuorumDatabase::DeleteOldRounds(uint64_t paxosID)
{
    uint64_t            oldPaxosID;
    int                 read;
    Buffer              key;
    StorageKeyValue*    kv;
    StorageCursor       cursor(table);
    
    // start at key "round:"
    // and delete up to "round:<paxosID - logCacheSize>"
    
    key.Writef("round:");
    kv = cursor.Begin(ReadBuffer(key));
    
    while (kv->key.BeginsWith("round:"))
    {
        read = kv->key.Readf("round:%U", &oldPaxosID);
        if (read < 7)
            break;
        if (oldPaxosID < (paxosID - logCacheSize))
            table->Delete(key);
        else
            break;
    }
    
    cursor.Close();
    
    Commit();
}
