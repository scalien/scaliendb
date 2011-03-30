#include "Test.h"
#include "Application/ConfigState/ConfigState.h"
#include "Application/ConfigServer/JSONConfigState.h"
#include "Application/HTTP/HTTPConnection.h"
#include "Application/HTTP/JSONSession.h"

static const char   configStateString[] = "C:Y:0:N:2:2:2:63:104:1:1:1:100:0:9:54:55:56:57:58:59:60:61:62:0:N:68664:Y:100:1:1:5:iglue:1:1:1:1:1:5:image:F:9:54:55:56:57:58:59:60:61:62:9:1:1:54:0::12:16490485.jpg:F:0:551741644:12:14225925.jpg:1:1:55:12:28765063.jpg:12:29270914.jpg:F:54:514338931:12:29015129.jpg:1:1:56:12:24053480.jpg:12:26506321.jpg:F:54:542674075:12:26267603.jpg:1:1:57:12:30532791.jpg:12:31077834.jpg:F:55:507739778:12:31077834.jpg:1:1:58:12:16490485.jpg:12:17422365.jpg:F:54:351727327:12:17422365.jpg:1:1:59:12:29270914.jpg:12:30532791.jpg:F:55:534271353:12:30017332.jpg:1:1:60:12:26506321.jpg:12:28765063.jpg:F:56:516223864:12:28509952.jpg:1:1:61:12:31077834.jpg:0::F:57:483864271:12:31077834.jpg:1:1:62:12:17422365.jpg:12:24053480.jpg:F:58:309724180:12:17422365.jpg:4:100:19:192.168.39.17:10020:8091:7091:0:101:19:192.168.39.16:10010:8090:7090:0:102:19:192.168.39.18:10000:8090:7090:0:103:19:192.168.39.19:10030:8092:7092:0";
static ReadBuffer   configStateBuffer(configStateString);

TEST_DEFINE(TestConfigStateJSON)
{
    ConfigState     configState;
    HTTPConnection  httpConnection;
    JSONSession     jsonSession;

    configState.Read(configStateBuffer, true);

    httpConnection.Init(NULL);
    jsonSession.Init(&httpConnection);

    JSONConfigState jsonConfigState(NULL, configState, jsonSession);
    jsonConfigState.Write();
    jsonSession.End();
    
    Buffer* jsonOutput = httpConnection.GetWriteBuffer();
    TEST_LOG("%.*s\n\n", jsonOutput->GetLength(), jsonOutput->GetBuffer());
    
    return TEST_SUCCESS;
}
