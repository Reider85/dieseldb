@echo off
setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

set PORT=%1
if "%PORT%"=="" set PORT=3306

set DATA_DIR=%2
if "%DATA_DIR%"=="" set DATA_DIR=.

set CLASSPATH=%SCRIPT_DIR%target\classes;%SCRIPT_DIR%

if exist "%SCRIPT_DIR%target\classpath.txt" (
    for /f "usebackq delims=" %%i in ("%SCRIPT_DIR%target\classpath.txt") do set DEPS=%%i
    if not "!DEPS!"=="" set CLASSPATH=!CLASSPATH!;!DEPS!
) else (
    if exist "%USERPROFILE%\.m2\repository\org\slf4j\slf4j-api\2.0.12\slf4j-api-2.0.12.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\org\slf4j\slf4j-api\2.0.12\slf4j-api-2.0.12.jar
    if exist "%USERPROFILE%\.m2\repository\ch\qos\logback\logback-classic\1.5.6\logback-classic-1.5.6.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\ch\qos\logback\logback-classic\1.5.6\logback-classic-1.5.6.jar
    if exist "%USERPROFILE%\.m2\repository\ch\qos\logback\logback-core\1.5.6\logback-core-1.5.6.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\ch\qos\logback\logback-core\1.5.6\logback-core-1.5.6.jar
)

if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"

echo Starting DieselDB server on port %PORT% with data dir %DATA_DIR%
java -cp "%CLASSPATH%" diesel.DatabaseServer %PORT% %DATA_DIR%
