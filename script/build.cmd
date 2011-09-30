@echo off
SET ARCH=%1
SET BIN_DIR=..\bin\%ARCH%
FOR /F %%a IN ('..\script\version.cmd ..\src\Version.h') DO SET VERSION=%%a
SET BUILD_DIR=..\build\%ARCH%\ScalienDB-%ARCH%-%VERSION%
echo "Creating build directory: %BUILD_DIR%"

RMDIR /S /Q %BUILD_DIR%
MD %BUILD_DIR%
COPY %BIN_DIR%\ScalienDB.exe %BUILD_DIR%
COPY %BIN_DIR%\scaliendb_client.dll %BUILD_DIR%
COPY %BIN_DIR%\ScalienClient.dll %BUILD_DIR%
COPY ..\test\control\release\controller.conf %BUILD_DIR%\controller.conf
COPY ..\test\shard\release\shard.conf %BUILD_DIR%\shard.conf
XCOPY /q /e /i ..\webadmin %BUILD_DIR%\webadmin
REM powershell -ExecutionPolicy RemoteSigned -File ..\script\buildzip.ps1 %ARCH% ..\build %VERSION%
