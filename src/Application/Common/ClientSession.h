#ifndef CLIENTSESSION_H
#define CLIENTSESSION_H

#include "System/Containers/InList.h"
#include "ClientRequest.h"

/*
===============================================================================================

 ClientSession
 
===============================================================================================
*/

class ClientSession
{
    typedef InList<ClientRequest> Transaction;

public:
    virtual ~ClientSession();
        
    virtual void    OnComplete(ClientRequest* request, bool last)       = 0;
    virtual bool    IsActive()                                          = 0;

    void            Init();
    bool            IsTransactional();
    bool            IsCommitting();
    
    Buffer          lockKey;
    Transaction     transaction;
};

#endif
