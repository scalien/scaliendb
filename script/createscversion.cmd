REM Create SourceControl.h for including Git commit hash into the program
@ECHO OFF
SET GITDIR=..\.git
SET GITHEADFILE=%GITDIR%\HEAD
SET OUTPUT=..\src\SourceControl.h

SETLOCAL ENABLEEXTENSIONS
FOR /F "tokens=2" %%a IN (
'FINDSTR /C:"ref:" %GITHEADFILE%'
) DO (
SET HEADREF=%%a
) 

FOR /F "tokens=1" %%a IN (
%GITDIR%\%HEADREF%
) DO (
SET COMMIT_ID=%%a
) 

FOR /f "tokens=1,2,3 delims=/" %%a IN ("%HEADREF%") DO SET REFDIR=%%a&SET REF=%%b&SET BRANCH=%%c

ECHO // Auto-generated file > %OUTPUT%
ECHO #ifndef SOURCE_CONTROL_H >> %OUTPUT%
ECHO #define SOURCE_CONTROL_H >> %OUTPUT%
ECHO. >> %OUTPUT%
ECHO #define SOURCE_CONTROL_VERSION "%COMMIT_ID%" >> %OUTPUT%
ECHO #define SOURCE_CONTROL_BRANCH "%BRANCH%" >> %OUTPUT%
ECHO. >> %OUTPUT%
ECHO #endif >> %OUTPUT%
ECHO. >> %OUTPUT%
ECHO SourceControl.h written successfully (%COMMIT_ID%).
