#include "Test.h"
#include "Application/ConfigState/ConfigState.h"
#include "Application/ConfigServer/JSONConfigState.h"
#include "Application/HTTP/JSONSession.h"

static const char   configStateString[] = "C:Y:0:N:3:2:2:243:104:1:1:3:100:102:103:1:101:16:227:228:229:230:231:232:233:234:235:236:237:238:239:240:241:242:0:N:136576:Y:102:1:1:5:iglue:1:1:1:1:1:5:image:F:16:227:228:229:230:231:232:233:234:235:236:237:238:239:240:241:242:16:1:1:227:0::12:14226507.jpg:F:0:364981241:12:14226507.jpg:1:1:228:12:28762053.jpg:12:29016389.jpg:F:227:349910804:12:28894655.jpg:1:1:229:12:24049310.jpg:12:26264013.png:F:227:383142763:12:25256862.jpg:1:1:230:12:30541035.jpg:12:30845325.jpg:F:228:348318549:12:30845325.jpg:1:1:231:12:16487287.jpg:12:17418875.jpg:F:227:460135866:12:17121085.jpg:1:1:232:12:26503311.jpg:12:28509561.jpg:F:229:359343915:12:28509564.jpg:1:1:233:12:29274406.jpg:12:30021696.jpg:F:228:360070703:12:30021792.jpg:1:1:234:12:31081808.jpg:12:31365436.jpg:F:230:322673242:12:31365436.jpg:1:1:235:12:17418875.jpg:12:24049310.jpg:F:231:423874378:12:17565731.jpg:1:1:236:12:26264013.png:12:26503311.jpg:F:229:349286652:12:26381675.jpg";
static ReadBuffer   configStateBuffer(configStateString);

TEST_DEFINE(TestConfigStateJSON)
{
    ConfigState         configState;
    JSONBufferWriter    jsonWriter;
    Buffer              jsonOutput;

    configState.Read(configStateBuffer, true);

    jsonWriter.Init(&jsonOutput);
    jsonWriter.Start();

    JSONConfigState jsonConfigState(NULL, configState, jsonWriter);
    jsonConfigState.Write();

    jsonWriter.End();
    
    TEST_LOG("%.*s\n\n", jsonOutput.GetLength(), jsonOutput.GetBuffer());
    
    return TEST_SUCCESS;
}
