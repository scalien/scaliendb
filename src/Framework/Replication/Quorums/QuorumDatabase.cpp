#include "QuorumDatabase.h"

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
    // TODO:
//  Buffer      key;
//  Buffer      value;
//  int         ret;
//
//  key.Write("accepted");
//  
//  ret = section->Get(NULL, key, value);
//  if (!ret)
//      return false;   // not found, return default
//  
//  if (value.GetLength() != 1)
//      return false;   // incorrect value, return default
//      
//  if (value.GetCharAt(0) == '1')
//      return true;
//  
    return false;
}

void QuorumDatabase::SetAccepted(bool accepted)
{
    // TODO:
//  Buffer      key;
//  Buffer      value;
//  int         ret;
//  
//  key.Write("accepted");
//  
//  value.Writef("%d", accepted);
//  ret = section->Set(transaction, key, value);
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
    // TODO:
//  Buffer      key;
//  int         ret;
//  
//  key.Write("acceptedValue");
//  
//  ret = section->Get(NULL, key, acceptedValue);
}

void QuorumDatabase::SetAcceptedValue(Buffer& acceptedValue)
{
    // TODO:
//  Buffer      key;
//  int         ret;
//
//  key.Write("acceptedValue");
//
//  ret = section->Set(transaction, key, acceptedValue);
}

void QuorumDatabase::GetLearnedValue(uint64_t paxosID, Buffer& value)
{
    // TODO:
//  Buffer key;
//  key.Writef("learnedValue:%U", paxosID);
//  section->Get(NULL, key, value);
}

void QuorumDatabase::SetLearnedValue(uint64_t paxosID, ReadBuffer& value)
{
    // TODO:
//  Buffer key;
//  
//  key.Writef("learnedValue:%U", paxosID);
//  section->Set(NULL, key.GetBuffer(), key.GetLength(), value.GetBuffer(), value.GetLength());
}

bool QuorumDatabase::IsActive()
{
    // TODO: for async write
    return false;
}

void QuorumDatabase::Commit()
{
    // TODO:
}

uint64_t QuorumDatabase::GetUint64(const char* name)
{
    // TODO:
//  Buffer      key;
//  Buffer      value;
//  uint64_t    u64;
//  unsigned    nread;
//  int         ret;
//  
//  key.Write(name);
//  
//  ret = section->Get(NULL, key, value);
//  if (!ret)
//      return false;
//
//  nread = 0;
//  u64 = BufferToUInt64(value.GetBuffer(), value.GetLength(), &nread);
//  if (nread != value.GetLength())
//  {
//      Log_Trace();
//      u64 = 0;
//  }
//  
//  return u64;
    return 0;
}

void QuorumDatabase::SetUint64(const char* name, uint64_t u64)
{
    // TODO:
//  Buffer  key;
//  Buffer  value;
//  int     ret;
//  
//  key.Write(name);
//  
//  value.Writef("%U", u64);
//  ret = section->Set(transaction, key, value);
}
