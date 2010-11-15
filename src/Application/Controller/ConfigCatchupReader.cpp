//#include "ConfigCatchupReader.h"
//#include "Application/ConfigState/ConfigState.h"
//#include "ConfigQuorumProcessor.h"
//#include "Controller.h"
//
//void ConfigCatchupReader::Init(ConfigQuorumProcessor* quorumProcessor_)
//{
//    quorumProcessor = quorumProcessor_;
//    isActive = false;
//}
//
//bool ConfigCatchupReader::IsActive()
//{
//    return isActive;
//}
//
//void ConfigCatchupReader::Begin()
//{
//    isActive = true;
//}
//
//void ConfigCatchupReader::Abort()
//{
//    isActive = false;
//}
//
//void ConfigCatchupReader::OnBeginShard(CatchupMessage& message)
//{
//    ASSERT_FAIL();
//}
//
//void ConfigCatchupReader::OnKeyValue(CatchupMessage& message)
//{
//    bool            hasMaster;
//    uint64_t        masterID;
//    ConfigState*    configState;
//    
//    if (!isActive)
//        return;
//    
//    configState = quorumProcessor->GetController()->GetConfigState();
//    hasMaster = configState->hasMaster;
//    masterID = configState->masterID;
//    if (!configState->Read(message.value))
//        ASSERT_FAIL();
//    configState->hasMaster = hasMaster;
//    configState->masterID = masterID;
//}
//
//void ConfigCatchupReader::OnCommit(CatchupMessage& message)
//{
//    if (!isActive)
//        return;
//
//    configStatePaxosID = message.paxosID;
//    WriteConfigState();
//    quorumProcessor->OnCatchupComplete(message.paxosID); // this commits
//    isActive = false;
//    configContext.ContinueReplication();
//
//    Log_Message("Catchup complete");
//}
//
//void ConfigCatchupReader::OnAbort(CatchupMessage& /*message*/)
//{
//    isActive = false;
//
//    Log_Message("Catchup aborted");
//}
