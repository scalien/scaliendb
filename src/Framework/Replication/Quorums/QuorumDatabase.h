#ifndef QUORUMDATABASE_H
#define QUORUMDATABASE_H

#include "System/Platform.h"
#include "System/Buffers/Buffer.h"
#include "Framework/Storage/StorageTable.h"

#define RLOG_CACHE_SIZE     100*1000
#define RLOG_REACTIVATION_DIFF  1*1000
//#define RLOG_CACHE_SIZE         10
//#define RLOG_REACTIVATION_DIFF  3

/*
===============================================================================================

 QuorumDatabase

===============================================================================================
*/

class QuorumDatabase
{
public:
    void                Init(StorageTable* table);

    uint64_t            GetPaxosID();
    void                SetPaxosID(uint64_t paxosID);
    
    bool                GetAccepted();
    void                SetAccepted(bool accepted);
    
    uint64_t            GetPromisedProposalID();
    void                SetPromisedProposalID(uint64_t promisedProposalID);
    
    uint64_t            GetAcceptedProposalID();
    void                SetAcceptedProposalID(uint64_t acceptedProposalID);
    
    uint64_t            GetAcceptedRunID();
    void                SetAcceptedRunID(uint64_t acceptedRunID);

    void                GetAcceptedValue(Buffer& acceptedValue);
    void                SetAcceptedValue(Buffer& acceptedValue);

    void                GetLearnedValue(uint64_t paxosID, Buffer& value);
    void                SetLearnedValue(uint64_t paxosID, ReadBuffer& value);

    bool                IsActive();
    
    void                Commit();

private:
    uint64_t            GetUint64(const char* name);
    void                SetUint64(const char* name, uint64_t value);
    
    void                DeleteOldRounds(uint64_t paxosID);
    
    StorageTable*       table;
    uint64_t            logCacheSize;
};

#endif
