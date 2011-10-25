@ECHO OFF
SET BASE_DIR=%1
SET FILENAME=%2
ECHO Copying DLL to Win32 release path
COPY %FILENAME% %BASE_DIR%\bin\Win32
ECHO Copying DLL to x64 release path
COPY %FILENAME% %BASE_DIR%\bin\x64
