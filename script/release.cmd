@ECHO OFF

SET OUTPUT=release.log

ECHO.
ECHO ============================================
ECHO.
ECHO Building C# client library
ECHO.
ECHO ============================================
ECHO.
SET BASEDIR=%CD%
CD src\Application\Client\CSharp\ScalienClient
CALL msbuild ScalienClient.csproj /v:minimal /t:Rebuild /p:Configuration=Debug
CHDIR /D %BASEDIR%

ECHO.
ECHO ============================================
ECHO.
ECHO Building ScalienDB and native client library
ECHO.
ECHO ============================================
ECHO.
CD ScalienDB.vcproj
CALL ..\script\buildrelease.cmd x64
CALL ..\script\buildrelease.cmd Win32
CHDIR /D %BASEDIR%

ECHO.
ECHO ============================================
ECHO.
ECHO Building combined C# client library
ECHO.
ECHO ============================================
ECHO.
CD src\Application\Client\CSharp\ScalienClientWithNativeDLL
CALL msbuild ScalienClientWithNativeDLL.csproj /v:minimal /t:Rebuild /p:Configuration=Debug
CHDIR /D %BASEDIR%

ECHO.
ECHO ============================================
ECHO.
ECHO Copying files to release directory
ECHO.
ECHO ============================================
ECHO.
CD ScalienDB.vcproj
CALL ..\script\copyrelease.cmd x64
CALL ..\script\copyrelease.cmd Win32
CHDIR /D %BASEDIR%

ECHO.
ECHO ============================================
ECHO.
ECHO Release is done.
ECHO.
ECHO ============================================
ECHO.

