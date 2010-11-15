//#ifndef CONFIGCATCHUPREADER_H
//#define CONFIGCATCHUPREADER_H
//
//#include "Application/Common/CatchupMessage.h"
//
//class ConfigQuorumProcessor;
//
///*
//===============================================================================================
//
// ConfigCatchupReader
//
//===============================================================================================
//*/
//
//class ConfigCatchupReader
//{
//public:
//    void                    Init(ConfigQuorumProcessor* quorumProcessor);
//    
//    bool                    IsActive();
//
//    void                    Begin();
//    void                    Abort();    
//    
//    void                    OnBeginShard(CatchupMessage& message);
//    void                    OnKeyValue(CatchupMessage& message);
//    void                    OnCommit(CatchupMessage& message);
//    void                    OnAbort(CatchupMessage& message);
//
//private:
//    bool                    isActive;
//    ConfigQuorumProcessor*  quorumProcessor;
//};
//
//#endif
