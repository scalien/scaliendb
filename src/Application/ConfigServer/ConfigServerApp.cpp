#include "ConfigServerApp.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/ClientRequestCache.h"
#include "Framework/Storage/StoragePageCache.h"
#include "System/Config.h"

void ConfigServerApp::Init()
{
    int         sdbpPort;
    Endpoint    httpEndpoint;
    ReadBuffer  docroot;
    ReadBuffer  prefix;
    ReadBuffer  index;
        
    httpHandler.SetConfigServer(&configServer);
    httpServer.Init(configFile.GetIntValue("http.port", 8080));
    httpServer.RegisterHandler(&httpHandler);
         
    docroot.Wrap(configFile.GetValue("http.documentRoot", "."));
    prefix.Wrap(configFile.GetValue("http.staticPrefix", "/webadmin"));
    index.Wrap(configFile.GetValue("http.directoryIndex", "index.html"));

    httpFileHandler.Init(docroot, prefix);
    httpFileHandler.SetDirectoryIndex(index);
    httpServer.RegisterHandler(&httpFileHandler);
    
    sdbpPort = configFile.GetIntValue("sdbp.port", 7080);
    sdbpServer.Init(sdbpPort);
    sdbpServer.SetContext(&configServer);
    sdbpServer.UseKeepAlive(false);

    // start configServer only after network servers are started
    configServer.Init(this);

    configServer.GetHTTPEndpoint(httpEndpoint);
    Log_Message("Web admin is started at http://%s%R", httpEndpoint.ToString(), &prefix);
    Log_Message("Waiting for connections on port %d", sdbpPort);
    
    logStatTimer.SetCallable(MFUNC(ConfigServerApp, OnLogStatTimer));
    // logStatTime is in seconds in the config file, default is 10min
    SetLogStatTimeout(configFile.GetIntValue("controller.logStatTime", 0) * 1000);
}

void ConfigServerApp::Shutdown()
{
    EventLoop::Remove(&logStatTimer);
    sdbpServer.Shutdown();
    httpServer.Shutdown();
    configServer.Shutdown();
}

void ConfigServerApp::SetLogStatTimeout(uint64_t timeout)
{
    EventLoop::Remove(&logStatTimer);
    logStatTimer.SetDelay(timeout);

    if (logStatTimer.GetDelay() != 0)
        EventLoop::Add(&logStatTimer);
}

void ConfigServerApp::OnLogStatTimer()
{
    Log_Debug("=== ConfigServer stats ===");
    Log_Debug("SDBP active: %u, inactive: %u", sdbpServer.GetNumActiveConns(), sdbpServer.GetNumInactiveConns());
    Log_Debug("HTTP active: %u, inactive: %u", httpServer.GetNumActiveConns(), httpServer.GetNumInactiveConns());
    Log_Debug("Cluster active: %u, write readyness: %u", CONTEXT_TRANSPORT->GetNumConns(), CONTEXT_TRANSPORT->GetNumWriteReadyness());
    Log_Debug("Heartbeats: %u", configServer.GetHeartbeatManager()->GetNumHeartbeats());
    Log_Debug("Primary leases: %u", configServer.GetPrimaryLeaseManager()->GetNumPrimaryLeases());
    Log_Debug("Config messages: %u", configServer.GetQuorumProcessor()->GetNumConfigMessages());
    Log_Debug("Requests: %u", configServer.GetQuorumProcessor()->GetNumRequests());
    Log_Debug("Listen requests: %u", configServer.GetQuorumProcessor()->GetNumListenRequests());
    Log_Debug("Timers: %u", EventLoop::GetNumTimers());
    Log_Debug("Page cache size: %s, num. pages: %u", HUMAN_BYTES(StoragePageCache::GetSize()), StoragePageCache::GetNumPages());
    Log_Debug("Request cache free list size: %u", REQUEST_CACHE->GetNumFreeRequests());
    EventLoop::Add(&logStatTimer);
}

unsigned ConfigServerApp::GetNumSDBPClients()
{
    return sdbpServer.GetNumActiveConns();
}
