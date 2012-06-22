#include "Test.h"
#include "System/FileSystem.h"
#include "Application/ConfigState/ConfigState.h"
#include "Application/ConfigServer/JSONConfigState.h"
#include "Application/HTTP/JSONSession.h"

static const char   configStateString[] = "C:Y:0:P253:N:3:2:3:32:102:2:1:1:100:0:0:0:N:21384:Y:100:2:1:101:0:1:31:0:N:32771:Y:101:1:1:4:bulk:1:2:1:1:2:4:pics:T:1:31:1:2:2:31:0::0::2:0:154360831:4:2197:2:100:15:127.0.0.1:10010:8090:7090:1:1:21384:F:0:0:0:101:15:127.0.0.1:10020:8091:7091:1:2:32771:F:0:0:0";
static ReadBuffer   configStateBuffer(configStateString);

TEST_DEFINE(TestConfigStateJSON)
{
    ConfigState         configState;
    JSONBufferWriter    jsonWriter;
    JSONConfigState     jsonConfigState;
    Buffer              jsonOutput;

    configState.Read(configStateBuffer, true);

    jsonWriter.Init(&jsonOutput);
    jsonWriter.Start();

    jsonConfigState.SetConfigState(&configState);
    jsonConfigState.SetJSONBufferWriter(&jsonWriter);
    jsonConfigState.Write();

    jsonWriter.End();
    
    TEST_LOG("%.*s\n\n", jsonOutput.GetLength(), jsonOutput.GetBuffer());
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestConfigStateCopy)
{
    ConfigState     configState;
    ConfigState     configStateCopy;
    Buffer          buffer;
    ReadBuffer      rb;
    FD              fd;
    int64_t         fileSize;

    fd = FS_Open("configState", FS_READONLY);
    fileSize = FS_FileSize(fd);
    buffer.Allocate(fileSize);

    FS_FileRead(fd, buffer.GetBuffer(), fileSize);
    FS_FileClose(fd);

    buffer.SetLength(fileSize);
    rb.Wrap(buffer);
    configState.Read(rb);

    /////////////////////
    // Profile from here
    /////////////////////

    TEST_LOG("File opened, configState read, sleeping 15 seconds...");
    MSleep(15 * 1000);

    for (int i = 0; i < 100000; i++)
    {
        configStateCopy = configState;
    }

    return TEST_SUCCESS;
}
