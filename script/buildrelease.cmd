: This script assumes that it is running from ScalienDB.vcproj directory
@ECHO OFF
SET ARCH=%1

: Build release versions of ScalienDB
CALL msbuild ScalienDB.vcxproj /t:Rebuild /p:Configuration=Release /p:Platform=%ARCH%

: Build release versions of Scalien client library
CALL msbuild ScalienClientLib.vcxproj /t:Rebuild /p:Configuration=Release /p:Platform=%ARCH%
CALL msbuild ScalienClientCSharpDLL.vcxproj /t:Build /p:Configuration=Release /p:Platform=%ARCH%

