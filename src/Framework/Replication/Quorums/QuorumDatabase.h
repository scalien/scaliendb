#ifndef QUORUMDATABASE_H
#define QUORUMDATABASE_H

#include "System/Platform.h"
#include "System/Buffers/Buffer.h"
#include "Framework/Storage/StorageTable.h"

/*
===============================================================================

 QuorumDatabase

===============================================================================
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
    
    StorageTable*       table;
};

#endif
