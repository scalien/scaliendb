@ECHO OFF
SET PROJECTNAME=%1
ECHO Starting prebuild script.
CALL ..\script\replaceversion.cmd %PROJECTNAME%.rc
CALL ..\script\createscversion.cmd
ECHO Prebuild script succeeded.
