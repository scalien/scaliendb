@ECHO OFF
REM Replace assembly version with the one from Version.h
SET BASE_DIR=%1
SET FILENAME=%2\%3
SET BARE_FILENAME=%3
SET SCRIPT_DIR=%BASE_DIR%\script
FOR /F "tokens=*" %%a IN ('FINDSTR /C:"[assembly: AssemblyVersion" %FILENAME%') DO (SET ASSEMBLY_VERSION=%%a)
FOR /F "tokens=*" %%a IN ('FINDSTR /C:"[assembly: AssemblyFileVersion" %FILENAME%') DO (SET ASSEMBLY_FILE_VERSION=%%a)
FOR /F %%a IN ('%SCRIPT_DIR%\version.cmd %BASE_DIR%\src\Version.h . -extra') DO SET DOTTED_VERSION=%%a
FOR /F %%a IN ('%SCRIPT_DIR%\version.cmd %BASE_DIR%\src\Version.h -comma -extra') DO SET COMMA_VERSION=%%a

IF "[assembly: AssemblyFileVersion("%DOTTED_VERSION%")]"=="%ASSEMBLY_FILE_VERSION%" GOTO DONE
ECHO Replacing assembly file version to %DOTTED_VERSION%.
PowerShell -ExecutionPolicy Bypass -File %SCRIPT_DIR%\replaceassemblyversion.ps1 %FILENAME% AssemblyFileVersion %DOTTED_VERSION% > %FILENAME%.temp
REM TODO: Change AssemblyVersion on major version change
IF %ERRORLEVEL% NEQ 0 GOTO REPLACE_ERROR
DEL %FILENAME%
REN %FILENAME%.temp %BARE_FILENAME%
:DONE
ECHO Assembly version is up-to-date (%DOTTED_VERSION%).
GOTO END
:REPLACE_ERROR
ECHO Cannot replace version!
GOTO END
:END
