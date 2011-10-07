@ECHO OFF
ECHO Starting prebuild script.
CALL ..\script\replaceversion.cmd ScalienDB.rc
CALL ..\script\createscversion.cmd
ECHO Prebuild script succeeded.
