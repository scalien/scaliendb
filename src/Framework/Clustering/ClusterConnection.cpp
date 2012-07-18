#include "ClusterConnection.h"
#include "ClusterTransport.h"
#include "Version.h"

static const unsigned keepAliveTimeout = 90*1000; // msec

ClusterConnection::ClusterConnection()
{
    UseKeepAlive(::keepAliveTimeout);
}

void ClusterConnection::InitConnected(bool startRead)
{
//    Buffer      buffer;
//    Buffer      versionString;
//    uint64_t    proto;
    
    MessageConnection::InitConnected(startRead);
    
    Log_Trace();

    progress = INCOMING;
    nodeID = UNDEFINED_NODEID;
    
    // TODO: HACK: this really should be a ClusterMessage, but it is at Application layer
    // If this changes, ClusterMessage should be changed too!
    
//    proto = 0;
//    versionString.Writef("ScalienDB cluster protocol, server version " VERSION_STRING "\n");
//    buffer.Writef("C:_:0:%#B", &versionString);
//    Write(buffer);
}

void ClusterConnection::SetTransport(ClusterTransport* transport_)
{
    Log_Trace();
    
    transport = transport_;
}

void ClusterConnection::SetNodeID(uint64_t nodeID_)
{
    nodeID = nodeID_;

    if (nodeID < 100)
        SetPriority(true);
    else
        SetPriority(false);
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

void ClusterConnection::Close()
{
    if (state == CONNECTED && nodeID != UNDEFINED_NODEID)
        Log_Message("[%s] Cluster node %U closed", endpoint.ToString(), nodeID);

    MessageConnection::Close();
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
    {
        buffer.Writef("%U:%U:%#R", 
         transport->GetClusterID(),
         transport->GetSelfNodeID(),
         &rb); // send my clusterID:nodeID:endpoint
    }
    Log_Trace("sending %B", &buffer);
    Write(buffer);
    
    Log_Trace("Conn READY to node %U at %s", nodeID, endpoint.ToString());

    transport->OnConnectionReady(nodeID, endpoint);

    if (nodeID != transport->GetSelfNodeID())
    {
        if (nodeID == UNDEFINED_NODEID)
            Log_Message("[%s] Cluster unknown node connected =>", endpoint.ToString());
        else
            Log_Message("[%s] Cluster node %U connected =>", endpoint.ToString(), nodeID);
    }
    else
        Log_Message("[%s] Cluster node connected to self", endpoint.ToString(), nodeID);

    progress = READY;
    OnWriteReadyness();
}

void ClusterConnection::OnClose()
{
    Log_Trace();
    
    if (connectTimeout.IsActive())
        return;
    
    MessageConnection::Close();
    if (nodeID != UNDEFINED_NODEID)
        Log_Message("[%s] Cluster node %U disconnected", endpoint.ToString(), nodeID);
    
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
        
        transport->OnConnectionEnd(nodeID, endpoint);
    }
}

bool ClusterConnection::OnMessage(ReadBuffer& msg)
{
    uint64_t            otherNodeID;
    uint64_t            otherClusterID;
    ReadBuffer          buffer;
    ClusterConnection*  dup;
    int                 read;

    //Log_Debug("ClusterConnection::OnMessage");

    if (progress == ClusterConnection::INCOMING)
    {
        // we have no incoming connections if we don't have a nodeID
        ASSERT(transport->IsAwaitingNodeID() == false);

        // the node at the other end is awaiting its nodeID
        if (msg.GetCharAt(0) == '*')
        {
            read = msg.Readf("*:%#R", &buffer);
            if (read != (int) msg.GetLength())
            {
                // protocol error
                GetSocket().GetEndpoint(endpoint);
                Log_Message("[%s] Cluster protocol error, disconnecting...", endpoint.ToString());
                transport->DeleteConnection(this);
                return true;
            }
            
            if (!endpoint.Set(buffer, true))
            {
                Log_Message("[%R] Cluster invalid network address", &buffer);
                transport->DeleteConnection(this);
                return true;                
            }
            
            progress = ClusterConnection::AWAITING_NODEID;
            Log_Trace("Conn is awaiting nodeID at %s", endpoint.ToString());

            transport->AddConnection(this);
            if (transport->OnAwaitingNodeID(endpoint))
            {
                Log_Trace();
                transport->DeleteConnection(this);
                return true;
            }
            if (nodeID == UNDEFINED_NODEID)
                Log_Message("[%s] Cluster unknown node connected <=", endpoint.ToString());
            else
                Log_Message("[%s] Cluster node %U connected <=", endpoint.ToString(), nodeID);
            return false;
        }
        
        // both ends have nodeIDs
        read = msg.Readf("%U:%U:%#R", &otherClusterID, &otherNodeID, &buffer);
        if (read != (int) msg.GetLength())
        {
            // protocol error
            GetSocket().GetEndpoint(endpoint);
            Log_Message("[%s] Cluster protocol error, disconnecting...", endpoint.ToString());
            transport->DeleteConnection(this);
            return true;
        }
        
        if (otherClusterID > 0)
        {
            if (transport->GetClusterID() == 0)
            {
                // the other side has a clusterID, I don't, so set mine
                // if I'm a config server:
                //   the clusterID also needs to be set in REPLICATON_CONFIG,
                //   this is set in ConfigServer::OnConnectionReady()
                // if I'm a shard server:
                //   the controllers will send a SetNodeID message, which
                //   contains the clusterID, so I'll be fine
                transport->SetClusterID(otherClusterID);
            }
            else
            {
                if (otherClusterID != transport->GetClusterID())
                {
                    Log_Message("[%R] Cluster invalid configuration, disconnecting...", &buffer);
                    Log_Debug("mine: %U != controller %U", transport->GetClusterID(), otherClusterID);
            
                    transport->DeleteConnection(this);          // drop this
                    return true;
                }
            }
        }
        
        dup = transport->GetConnection(otherNodeID);
        if (dup)
        {
            // if the other connections isn't ready yet, drop it
            // OR
            // the other connection is ready
            // in this case, kill the connection that was initiated by higher nodeID
            // in other words, since this is an incoming connection:
            // if nodeID[of initiator] > transport->GetSelfNodeID(): drop

            if (dup->progress != READY || otherNodeID > transport->GetSelfNodeID())
            {
                Log_Trace("delete dup");
                transport->DeleteConnection(dup);       // drop dup
            }
            else if (otherNodeID != transport->GetSelfNodeID())
            {
                Log_Trace("delete this");
                transport->DeleteConnection(this);      // drop this
                return true;
            }
        }
        progress = ClusterConnection::READY;
        SetNodeID(otherNodeID);
        if (!endpoint.Set(buffer, true))
        {
            Log_Message("[%R] Cluster invalid network address", &buffer);
            transport->DeleteConnection(this);
            return true;                
        }

        // check if the other side is not sending its localhost address, when they are on 
        // different nodes
        if (endpoint.GetAddress() == Endpoint::GetLoopbackAddress() && 
         transport->GetSelfEndpoint().GetAddress() != Endpoint::GetLoopbackAddress())
        {
            Log_Message("[%R] Cluster invalid network address", &buffer);
            transport->DeleteConnection(this);
            return true;
        }
        
        Log_Trace("Conn READY to node %U at %s", nodeID, endpoint.ToString());
        if (nodeID != transport->GetSelfNodeID())
        {
            if (nodeID == UNDEFINED_NODEID)
                Log_Message("[%s] Cluster unknown node connected <=", endpoint.ToString());
            else
                Log_Message("[%s] Cluster node %U connected <=", endpoint.ToString(), nodeID);
        }
        
        transport->AddConnection(this);
        transport->OnWriteReadyness(this);
        transport->OnConnectionReady(nodeID, endpoint);
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
