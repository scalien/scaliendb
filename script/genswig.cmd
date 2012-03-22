ECHO ON

SET CSHARP_NAMESPACE=Scalien
SET SRC_DIR=src
SET CLIENT_DIR=Application\Client
SET CSHARP_CLIENT_DIR=%CLIENT_DIR%\CSharp
SET OUTPUT_FILE=scaliendb_client_csharp.cpp
SET OUTPUT=%SRC_DIR%\%CSHARP_CLIENT_DIR%\%OUTPUT_FILE%
SET SCRIPT_DIR=script

swig -csharp -c++ -namespace %CSHARP_NAMESPACE% -outdir %SRC_DIR%/%CSHARP_CLIENT_DIR%/ScalienClient -o %OUTPUT% -I%SRC_DIR%/%CSHARP_CLIENT_DIR% %SRC_DIR%/%CLIENT_DIR%/scaliendb_client.i
IF %ERRORLEVEL% NEQ 0 GOTO ERROR

: Fixes SWIG Windows 64 bit intptr_t issue
SET FIXED_OUTPUT=%OUTPUT%.fixed
PowerShell -ExecutionPolicy Bypass -File %SCRIPT_DIR%\fix_swig_csharp.ps1 %OUTPUT% > %FIXED_OUTPUT%
IF %ERRORLEVEL% NEQ 0 GOTO ERROR
DEL %OUTPUT%
IF %ERRORLEVEL% NEQ 0 GOTO ERROR
REN %FIXED_OUTPUT% %OUTPUT_FILE%
IF %ERRORLEVEL% NEQ 0 GOTO ERROR

:END
EXIT /B 0

:ERROR
ECHO Error occured while generating wrapper!
EXIT /B 1

