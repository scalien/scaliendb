: This script assumes that it is running from ScalienDB.vcproj directory
@ECHO OFF
SET ARCH=%1

ECHO.
ECHO ============================================
ECHO.
ECHO Building ScalienDB %ARCH%
ECHO.
ECHO ============================================
ECHO.
CALL msbuild ScalienDB.vcxproj /v:minimal /t:Rebuild /p:Configuration=Release /p:Platform=%ARCH%

ECHO.
ECHO ============================================
ECHO.
ECHO Building Scalien native client %ARCH%
ECHO.
ECHO ============================================
ECHO.
CALL msbuild ScalienClientLib.vcxproj /v:minimal /t:Rebuild /p:Configuration=Release /p:Platform=%ARCH%

ECHO.
ECHO ============================================
ECHO.
ECHO Building Scalien client DLL for %ARCH%
ECHO.
ECHO ============================================
ECHO.
CALL msbuild ScalienClientCSharpDLL.vcxproj /v:minimal /t:Build /p:Configuration=Release /p:Platform=%ARCH%

