@echo off
SET ARCH=%1
SET BUILD_BASE_DIR=%~f2
IF "%BUILD_BASE_DIR%"=="" SET BUILD_BASE_DIR=..\build
SET BIN_DIR=..\bin\%ARCH%
FOR /F %%a IN ('..\script\version.cmd ..\src\Version.h') DO SET VERSION=%%a
SET BUILD_NAME=ScalienDB-%ARCH%-%VERSION%
SET BUILD_ARCH_DIR=%BUILD_BASE_DIR%\%ARCH%\
SET BUILD_DIR="%BUILD_ARCH_DIR%%BUILD_NAME%"
ECHO Creating build directory: %BUILD_DIR%

IF NOT EXIST %BUILD_DIR% GOTO COPYTOBUILD
ECHO Build directory already exists, exiting!
EXIT /B 1

:COPYTOBUILD

MD %BUILD_DIR%
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

COPY %BIN_DIR%\ScalienDB.exe %BUILD_DIR%
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

COPY %BIN_DIR%\scaliendb_client.dll %BUILD_DIR%
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

COPY %BIN_DIR%\ScalienClient.dll %BUILD_DIR%
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

COPY ..\test\control\release\controller.conf %BUILD_DIR%\controller.conf
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

COPY ..\test\shard\release\shard.conf %BUILD_DIR%\shard.conf
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

XCOPY /q /e /i ..\webadmin %BUILD_DIR%\webadmin
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

CALL ..\script\ziprelease.cmd "%BUILD_ARCH_DIR%" %BUILD_NAME%
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

ECHO %BUILD_NAME% > %BUILD_ARCH_DIR%/LatestVersion
