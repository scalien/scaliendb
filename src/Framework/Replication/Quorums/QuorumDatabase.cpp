#include "QuorumDatabase.h"
#include "Framework/Storage/StorageShardProxy.h"
#include "Framework/Storage/StorageEnvironment.h"

void QuorumDatabase::Init(StorageShardProxy* paxosShard_, StorageShardProxy* logShard_)
{
    ASSERT(paxosShard_ != NULL);
    ASSERT(logShard_ != NULL);
    paxosShard = paxosShard_;
    logShard = logShard_;
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

    ret = paxosShard->Get(rbKey, rbValue);
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
    
    ret = paxosShard->Set(rbKey, rbValue);
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

void QuorumDatabase::GetAcceptedValue(uint64_t paxosID, Buffer& value)
{
    Buffer      key;
    ReadBuffer  rbKey;
    ReadBuffer  rbValue;
    bool        ret;

    key.Writef("accepted:%021U", paxosID);
    rbKey.Wrap(key);
    
    ret = logShard->Get(rbKey, rbValue);
    if (!ret)
        return;

    ASSERT(rbValue.GetLength() > 0);
    
    value.Write(rbValue);
}

void QuorumDatabase::SetAcceptedValue(uint64_t paxosID, ReadBuffer value)
{
    Buffer      key;
    ReadBuffer  rbKey;

    ASSERT(value.GetLength() > 0);

    key.Writef("accepted:%021U", paxosID);
    rbKey.Wrap(key);

    logShard->Set(rbKey, value);
}

bool QuorumDatabase::IsCommiting()
{
    return paxosShard->GetEnvironment()->IsCommiting();
}

void QuorumDatabase::Commit()
{
    paxosShard->GetEnvironment()->Commit();
}

void QuorumDatabase::Commit(Callable& onCommit)
{
    paxosShard->GetEnvironment()->Commit(onCommit);
}

uint64_t QuorumDatabase::GetUint64(const char* name)
{
    ReadBuffer  key(name);
    ReadBuffer  value;
    uint64_t    u64;
    unsigned    nread;
    int         ret;

    ret = paxosShard->Get(key, value);
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

    ret = paxosShard->Set(key, value);
}
