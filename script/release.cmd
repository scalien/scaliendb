@ECHO OFF

SET BASEDIR=%CD%
CD src\Application\Client\CSharp\ScalienClient
CALL msbuild ScalienClient.csproj /t:Rebuild /p:Configuration=Debug
CHDIR /D %BASEDIR%

CD ScalienDB.vcproj
CALL ..\script\buildrelease.cmd x64
CALL ..\script\buildrelease.cmd Win32
CHDIR /D %BASEDIR%

CD src\Application\Client\CSharp\ScalienClientWithNativeDLL
CALL msbuild ScalienClientWithNativeDLL.csproj /t:Rebuild /p:Configuration=Debug
CHDIR /D %BASEDIR%

CD ScalienDB.vcproj
CALL ..\script\copyrelease.cmd x64
CALL ..\script\copyrelease.cmd Win32
CHDIR /D %BASEDIR%

