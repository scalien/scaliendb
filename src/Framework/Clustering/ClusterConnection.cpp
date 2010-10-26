#include "ClusterConnection.h"
#include "ClusterTransport.h"

void ClusterConnection::InitConnected(bool startRead)
{
    MessageConnection::InitConnected(startRead);
    
    Log_Trace();

    progress = INCOMING;
    nodeID = 0;
}

void ClusterConnection::SetTransport(ClusterTransport* transport_)
{
    Log_Trace();
    
    transport = transport_;
}

void ClusterConnection::SetNodeID(uint64_t nodeID_)
{
    nodeID = nodeID_;
}

void ClusterConnection::SetEndpoint(Endpoint& endpoint_)
{
    endpoint = endpoint_;
}

void ClusterConnection::SetProgress(Progress progress_)
{
    progress = progress_;
}

uint64_t ClusterConnection::GetNodeID()
{
    return nodeID;
}

Endpoint ClusterConnection::GetEndpoint()
{
    return endpoint;
}

ClusterConnection::Progress ClusterConnection::GetProgress()
{
    return progress;
}

void ClusterConnection::Connect()
{
    progress = OUTGOING;
    MessageConnection::Connect(endpoint);
}

void ClusterConnection::OnConnect()
{
    Buffer      buffer;
    ReadBuffer  rb;

    MessageConnection::OnConnect();
    
    Log_Trace("endpoint = %s", endpoint.ToString());
    
    rb = transport->GetSelfEndpoint().ToReadBuffer();
    
    if (transport->IsAwaitingNodeID())
        buffer.Writef("*:%#R", &rb); // send *:endpoint
    else
        buffer.Writef("%U:%#R", transport->GetSelfNodeID(), &rb); // send my nodeID:endpoint
    Log_Trace("sending %.*s", P(&buffer));
    Write(buffer);
    
    Log_Trace("Conn READY to node %" PRIu64 " at %s", nodeID, endpoint.ToString());

    progress = READY;
    OnWriteReadyness();
}

void ClusterConnection::OnClose()
{
    Log_Trace();
    
    if (connectTimeout.IsActive())
        return;
    
    Log_Message("[%s]: [%" PRIu64 "] Node disconnected", endpoint.ToString(), nodeID);
    MessageConnection::Close();
    
    if (progress == INCOMING)
    {
        // we don't know the other side, delete conn
        transport->DeleteConnection(this);
    }
    else if (progress == READY)
    {
        // endpoint contains the other side, connect
        progress = OUTGOING;
        
        EventLoop::Reset(&connectTimeout);
    }
}

bool ClusterConnection::OnMessage(ReadBuffer& msg)
{
    uint64_t            nodeID_;
    ReadBuffer          buffer;
    ClusterConnection*  dup;

    if (progress == ClusterConnection::INCOMING)
    {
        // we have no incoming connections if we don't have a nodeID
        assert(transport->IsAwaitingNodeID() == false);

        // the node at the other end is awaiting its nodeID
        if (msg.GetCharAt(0) == '*')
        {
            msg.Readf("*:%#R", &buffer);
            endpoint.Set(buffer);
            progress = ClusterConnection::AWAITING_NODEID;
            Log_Trace("Conn is awaiting nodeID at %s", endpoint.ToString());
            transport->AddConnection(this);
            if (transport->OnAwaitingNodeID(endpoint))
            {
                Log_Trace();
                transport->DeleteConnection(this);
                return true;
            }
            return false;
        }
        
        // both ends have nodeIDs
        msg.Readf("%U:%#R", &nodeID_, &buffer);
        dup = transport->GetConnection(nodeID_);
        if (dup)
        {
            // if the other connections isn't ready yet, drop it
            // OR
            // the other connection is ready
            // in this case, kill the connection that was initiated by higher nodeID
            // in other words, since this is an incoming connection:
            // if nodeID[of initiator] > transport->GetSelfNodeID(): drop

            if (dup->progress != READY || nodeID_ > transport->GetSelfNodeID())
            {
                Log_Trace("delete dup");
                transport->DeleteConnection(dup);       // drop dup
            }
            else if (nodeID_ != transport->GetSelfNodeID())
            {
                Log_Trace("delete this");
                transport->DeleteConnection(this);      // drop this
                return true;
            }
        }
        progress = ClusterConnection::READY;
        nodeID = nodeID_;
        endpoint.Set(buffer);
        Log_Trace("Conn READY to node %" PRIu64 " at %s", nodeID, endpoint.ToString());
        Log_Message("[%s]: [%" PRIu64 "] Node connected", endpoint.ToString(), nodeID);
        transport->AddConnection(this);
        transport->OnConnectionReady(nodeID, endpoint);
        transport->OnWriteReadyness(this);
    }
    else if (progress == ClusterConnection::OUTGOING)
        ASSERT_FAIL();
    else
        transport->OnMessage(nodeID, msg); // pass msg to upper layer
    
    return false;
}

void ClusterConnection::OnWriteReadyness()
{
    if (progress != ClusterConnection::READY)
        return;
    
    transport->OnWriteReadyness(this);
}
