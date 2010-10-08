#ifndef QUORUMCONTEXT_H
#define QUORUMCONTEXT_H

#include "System/Events/Callable.h"
#include "Quorum.h"
#include "QuorumDatabase.h"
#include "QuorumTransport.h"

/*
===============================================================================

 QuorumContext
 
===============================================================================
*/

class QuorumContext
{
public:
    virtual ~QuorumContext() {};

    virtual bool                IsLeaseOwner()                                  = 0;    
    virtual bool                IsLeaseKnown()                                  = 0;
    virtual uint64_t            GetLeaseOwner()                                 = 0;
    // multiPaxos    
    virtual bool                IsLeader()                                      = 0;

    virtual void                OnLearnLease()                                  = 0;
    virtual void                OnLeaseTimeout()                                = 0;

    virtual uint64_t            GetQuorumID()                                   = 0;
    virtual void                SetPaxosID(uint64_t paxosID)                    = 0;
    virtual uint64_t            GetPaxosID()                                    = 0;
    virtual uint64_t            GetHighestPaxosID()                             = 0;

    virtual Quorum*             GetQuorum()                                     = 0;
    virtual QuorumDatabase*     GetDatabase()                                   = 0;
    virtual QuorumTransport*    GetTransport()                                  = 0;
    
    virtual Buffer*             GetNextValue()                                  = 0;

    virtual void                OnAppend(ReadBuffer value, bool ownAppend)      = 0;
    virtual void                OnMessage(ReadBuffer msg)                       = 0;
    virtual void                OnStartCatchup()                                = 0;
    virtual void                OnCatchupComplete()                             = 0;

    virtual void                StopReplication()                               = 0;
    virtual void                ContinueReplication()                           = 0;
};

#endif
