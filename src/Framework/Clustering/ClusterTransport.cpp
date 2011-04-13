#include "ClusterTransport.h"

ClusterTransport::~ClusterTransport()
{
    conns.DeleteList();
    writeReadynessList.DeleteList();
}

void ClusterTransport::Init(Endpoint& endpoint_)
{
    endpoint = endpoint_;
    server.Init(endpoint.GetPort());
    server.SetTransport(this);
    awaitingNodeID = true;
    nodeID = UNDEFINED_NODEID;
    clusterID = 0;
}

void ClusterTransport::SetSelfNodeID(uint64_t nodeID_)
{
    nodeID = nodeID_;
    awaitingNodeID = false;
    ReconnectAll(); // so we establish "regular" connections with the nodeID
    server.Listen();
}

void ClusterTransport::SetClusterID(uint64_t clusterID_)
{
    clusterID = clusterID_;
}

bool ClusterTransport::IsAwaitingNodeID()
{
    return awaitingNodeID;
}

uint64_t ClusterTransport::GetSelfNodeID()
{
    return nodeID;
}

Endpoint& ClusterTransport::GetSelfEndpoint()
{
    return endpoint;
}

uint64_t ClusterTransport::GetClusterID()
{
    return clusterID;
}

unsigned ClusterTransport::GetNumConns()
{
    return conns.GetLength();
}

unsigned ClusterTransport::GetNumWriteReadyness()
{
    return writeReadynessList.GetLength();
}

void ClusterTransport::AddNode(uint64_t nodeID, Endpoint& endpoint)
{
    ClusterConnection* conn;
    
    conn = GetConnection(nodeID);
    if (conn != NULL)
        return;

    conn = new ClusterConnection;
    conn->SetTransport(this);
    conn->SetNodeID(nodeID);
    conn->SetEndpoint(endpoint);
    conn->Connect();
    conns.Append(conn);

    if (!awaitingNodeID && nodeID == this->nodeID)
        Log_Trace("connecting to self");
}

bool ClusterTransport::SetConnectionNodeID(Endpoint& endpoint, uint64_t nodeID)
{
    ClusterConnection* it;
    ClusterConnection* tdb;
    
    for (it = conns.First(); it != NULL;)
    {
        if (it->GetEndpoint() == endpoint)
        {
            if (it->GetProgress() != ClusterConnection::AWAITING_NODEID)
            {
                // the node had its db cleared, and this is our old connection to it
                tdb = it;
                it = conns.Next(it);
                DeleteConnection(tdb);
            }
            else
            {
                it->SetNodeID(nodeID);
                it->SetProgress(ClusterConnection::READY);
                return true;
            }
        }
        else
            it = conns.Next(it);
    }
    
    return false;
}

void ClusterTransport::SendMessage(uint64_t nodeID, Buffer& prefix, Message& msg)
{
    ClusterConnection*  conn;
    
    conn = GetConnection(nodeID);
    
    if (!conn)
    {
        Log_Trace("no connection to nodeID %U", nodeID);
        return;
    }
    
    if (conn->GetProgress() != ClusterConnection::READY)
    {
        Log_Trace("connection to %U has progress: %d", nodeID, conn->GetProgress());
        return;
    }
    
    msg.Write(msgBuffer);
    conn->Write(prefix, msgBuffer);
}

void ClusterTransport::DropConnection(uint64_t nodeID)
{
    ClusterConnection* conn;
    
    conn = GetConnection(nodeID);
    
    if (!conn)
        return;
        
    DeleteConnection(conn);
}

void ClusterTransport::DropConnection(Endpoint endpoint)
{
    ClusterConnection* conn;
    
    conn = GetConnection(endpoint);
    
    if (!conn)
        return;
        
    DeleteConnection(conn);
}

bool ClusterTransport::GetNextWaiting(Endpoint& endpoint)
{
    ClusterConnection* it;
    
    FOREACH (it, conns)
    {
        if (it->GetProgress() == ClusterConnection::AWAITING_NODEID)
        {
            endpoint = it->GetEndpoint();
            return true;
        }
    }
    
    return false;
}

bool ClusterTransport::IsConnected(uint64_t nodeID)
{
    ClusterConnection* it;
    
    FOREACH (it, conns)
    {
        if (it->GetNodeID() == nodeID)
        {
            if (it->GetProgress() == ClusterConnection::READY)
                return true;
            else
                return false;
        }
    }
    
    return false;
}

void ClusterTransport::RegisterWriteReadyness(uint64_t nodeID, Callable callable)
{
    WriteReadyness*     wr;
    ClusterConnection*  itConnection;
   
    wr = new WriteReadyness();
    wr->nodeID = nodeID;
    wr->callable = callable;
    
    writeReadynessList.Append(wr);
    
    FOREACH (itConnection, conns)
    {
        if (itConnection->GetNodeID() == nodeID &&
         itConnection->GetProgress() == ClusterConnection::READY)
        {
            Call(callable);
            return;
        }
    }
}

void ClusterTransport::UnregisterWriteReadyness(uint64_t nodeID, Callable callable)
{
    WriteReadyness* it;
    
    FOREACH(it, writeReadynessList)
    {
        if (it->nodeID == nodeID && it->callable == callable)
        {
            writeReadynessList.Delete(it);
            return;
        }
    }
}

void ClusterTransport::PauseReads(uint64_t nodeID)
{
    ClusterConnection* conn;
    
    conn = GetConnection(nodeID);
    
    if (!conn)
        return;
    
    conn->PauseReads();
}

void ClusterTransport::ResumeReads(uint64_t nodeID)
{
    ClusterConnection* conn;
    
    conn = GetConnection(nodeID);
    
    if (!conn)
        return;
    
    conn->ResumeReads();
}

void ClusterTransport::AddConnection(ClusterConnection* conn)
{
    conns.Append(conn);
}

ClusterConnection* ClusterTransport::GetConnection(uint64_t nodeID)
{
    ClusterConnection* it;
    
    FOREACH (it, conns)
    {
        if (it->GetNodeID() == nodeID && it->GetProgress() != ClusterConnection::AWAITING_NODEID)
            return it;
    }
    
    return NULL;
}

ClusterConnection* ClusterTransport::GetConnection(Endpoint& endpoint)
{
    ClusterConnection* it;
    
    FOREACH (it, conns)
    {
        if (it->GetEndpoint() == endpoint)
            return it;
    }
    
    return NULL;
}

void ClusterTransport::DeleteConnection(ClusterConnection* conn)
{
    conn->Close();

    if (conn->next != conn)
        conns.Remove(conn);

    delete conn;
}

void ClusterTransport::ReconnectAll()
{
    ClusterConnection* it;
    
    FOREACH (it, conns)
    {
        it->Close();
        it->Connect();
    }
}

void ClusterTransport::OnWriteReadyness(ClusterConnection* conn)
{
    WriteReadyness* it;
    
    ASSERT(conn->progress == ClusterConnection::READY);
    
    FOREACH(it, writeReadynessList)
    {
        if (it->nodeID == conn->nodeID)
        {
            Call(it->callable);
            return;
        }
    }
}