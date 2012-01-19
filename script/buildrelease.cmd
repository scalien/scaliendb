: This script assumes that it is running from ScalienDB.vcproj directory
@ECHO OFF
SET ARCH=%1

: Possible values are: quiet, minimal, normal, detailed, diagnostic
IF "%VERBOSITY%"=="" SET VERBOSITY=normal

: Possible values are: Rebuild, Build
IF "%TARGET%"=="" SET TARGET=Rebuild

: Possible values are: Release, Debug
IF "%CONFIGURATION%"=="" SET CONFIGURATION=Release

: Clean target directory before building
DEL /Q ..\bin\%ARCH%\*.* 

ECHO.
ECHO ============================================
ECHO.
ECHO Building ScalienDB %ARCH%
ECHO.
ECHO ============================================
ECHO.
CALL msbuild ScalienDB.vcxproj /v:%VERBOSITY% /t:%TARGET% /p:Configuration=%CONFIGURATION% /p:Platform=%ARCH%
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

ECHO.
ECHO ============================================
ECHO.
ECHO Building Scalien native client %ARCH%
ECHO.
ECHO ============================================
ECHO.
CALL msbuild ScalienClientLib.vcxproj /v:%VERBOSITY% /t:%TARGET% /p:Configuration=%CONFIGURATION% /p:Platform=%ARCH%
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

ECHO.
ECHO ============================================
ECHO.
ECHO Building Scalien client DLL for %ARCH%
ECHO.
ECHO ============================================
ECHO.
CALL msbuild ScalienClientCSharpDLL.vcxproj /v:%VERBOSITY% /t:Build /p:Configuration=%CONFIGURATION% /p:Platform=%ARCH%
IF %ERRORLEVEL% NEQ 0 EXIT /B 1
