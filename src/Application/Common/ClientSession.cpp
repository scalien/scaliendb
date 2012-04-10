#include "ClientSession.h"

ClientSession::~ClientSession()
{
}

void ClientSession::Init()
{
    lockKey.Clear();
    transaction.Clear();
}

bool ClientSession::IsTransactional()
{
    return lockKey.GetLength() > 0;
}

bool ClientSession::IsCommitting()
{
    if (transaction.Last() == NULL)
        return false;
    return (transaction.Last()->type == CLIENTREQUEST_COMMIT_TRANSACTION);
}
