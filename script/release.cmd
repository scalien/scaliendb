@ECHO OFF

SET SCRIPTDIR=%~dp0
SET BASEDIR=%SCRIPTDIR%..\
SET STARTDIR=%CD%
SET OUTPUT=release.log
: Possible values are: quiet, minimal, normal, detailed, diagnostic
SET VERBOSITY=minimal

SET BUILDDIR=%BASEDIR%%1
IF "%1"=="" SET BUILDDIR=%BASEDIR%build

CD %BASEDIR%

ECHO.
ECHO ============================================
ECHO.
ECHO Building ScalienDB and native client library
ECHO.
ECHO ============================================
ECHO.
CD ScalienDB.vcproj
CALL ..\script\buildrelease.cmd x64
IF %ERRORLEVEL% NEQ 0 GOTO ERROR
CALL ..\script\buildrelease.cmd Win32
IF %ERRORLEVEL% NEQ 0 GOTO ERROR
CHDIR /D %BASEDIR%

ECHO.
ECHO ============================================
ECHO.
ECHO Building combined C# client library
ECHO.
ECHO ============================================
ECHO.
CD src\Application\Client\CSharp\ScalienClientWithNativeDLL
CALL msbuild ScalienClientWithNativeDLL.csproj /v:%VERBOSITY% /t:Rebuild /p:Configuration=Debug
IF %ERRORLEVEL% NEQ 0 GOTO ERROR
CHDIR /D %BASEDIR%

ECHO.
ECHO ============================================
ECHO.
ECHO Copying files to release directory
ECHO.
ECHO ============================================
ECHO.
CD ScalienDB.vcproj
CALL ..\script\copyrelease.cmd x64 %BUILDDIR%
IF %ERRORLEVEL% NEQ 0 GOTO ERROR
CALL ..\script\copyrelease.cmd Win32 %BUILDDIR%
IF %ERRORLEVEL% NEQ 0 GOTO ERROR
CHDIR /D %BASEDIR%

ECHO.
ECHO ============================================
ECHO.
ECHO Copying symbol files to debug directory
ECHO.
ECHO ============================================
ECHO.
CD ScalienDB.vcproj
CALL ..\script\copyreleasedebuginfo.cmd x64 %BUILDDIR%
IF %ERRORLEVEL% NEQ 0 GOTO ERROR
CALL ..\script\copyreleasedebuginfo.cmd Win32 %BUILDDIR%
IF %ERRORLEVEL% NEQ 0 GOTO ERROR
CHDIR /D %BASEDIR%

ECHO.
ECHO ============================================
ECHO.
ECHO Release is done.
ECHO.
ECHO ============================================
ECHO.
EXIT /B 0

:ERROR
ECHO.
ECHO ============================================
ECHO.
ECHO An error occured!
ECHO.
ECHO ============================================
ECHO.
CHDIR /D %STARTDIR%
EXIT /B 1
