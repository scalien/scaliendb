#include "ConfigServerApp.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/ClientRequestCache.h"
#include "Framework/Storage/StoragePageCache.h"
#include "System/Config.h"

void ConfigServerApp::Init()
{
    ReadBuffer  docroot;
    ReadBuffer  prefix;
    ReadBuffer  index;
    
    configServer.Init();
    
    httpHandler.SetConfigServer(&configServer);
    httpServer.Init(configFile.GetIntValue("http.port", 8080));
    httpServer.RegisterHandler(&httpHandler);
         
    docroot.Wrap(configFile.GetValue("http.documentRoot", "."));
    prefix.Wrap(configFile.GetValue("http.staticPrefix", "/webadmin"));
    index.Wrap(configFile.GetValue("http.directoryIndex", "index.html"));

    httpFileHandler.Init(docroot, prefix);
    httpFileHandler.SetDirectoryIndex(index);
    httpServer.RegisterHandler(&httpFileHandler);
    
    sdbpServer.Init(configFile.GetIntValue("sdbp.port", 7080));
    sdbpServer.SetContext(&configServer);
    
    statTimer.SetDelay(configFile.GetIntValue("controller.logStatTime", 10*1000));
    statTimer.SetCallable(MFUNC(ConfigServerApp, OnStatTimer));
    EventLoop::Add(&statTimer);
}

void ConfigServerApp::Shutdown()
{
    EventLoop::Remove(&statTimer);
    sdbpServer.Shutdown();
    httpServer.Shutdown();
    configServer.Shutdown();
}

void ConfigServerApp::OnStatTimer()
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
    Log_Debug("Bufferpool available size: %s", HUMAN_BYTES(DEFAULT_BUFFERPOOL->GetAvailableSize()));
    Log_Debug("Request cache free list size: %u", REQUEST_CACHE->GetNumFreeRequests());
    EventLoop::Add(&statTimer);
}