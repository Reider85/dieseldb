@echo off
setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

set PORT=%1
if "%PORT%"=="" set PORT=3306

set DATA_DIR=%2
if "%DATA_DIR%"=="" set DATA_DIR=data

set CLASSPATH=%SCRIPT_DIR%target\classes;%SCRIPT_DIR%

if exist "%SCRIPT_DIR%target\classpath.txt" (
    for /f "usebackq delims=" %%i in ("%SCRIPT_DIR%target\classpath.txt") do set DEPS=%%i
    if not "!DEPS!"=="" set CLASSPATH=!CLASSPATH!;!DEPS!
) else (
    where mvn >nul 2>nul
    if not errorlevel 1 (
        mvn -q dependency:build-classpath -Dmdep.outputFile=target\classpath.txt -Dmdep.includeScope=runtime >nul 2>nul
        if exist "%SCRIPT_DIR%target\classpath.txt" (
            for /f "usebackq delims=" %%i in ("%SCRIPT_DIR%target\classpath.txt") do set DEPS=%%i
            if not "!DEPS!"=="" set CLASSPATH=!CLASSPATH!;!DEPS!
        )
    )
    if not defined DEPS (
        if exist "%USERPROFILE%\.m2\repository\org\slf4j\slf4j-api\2.0.12\slf4j-api-2.0.12.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\org\slf4j\slf4j-api\2.0.12\slf4j-api-2.0.12.jar
        if exist "%USERPROFILE%\.m2\repository\ch\qos\logback\logback-classic\1.5.6\logback-classic-1.5.6.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\ch\qos\logback\logback-classic\1.5.6\logback-classic-1.5.6.jar
        if exist "%USERPROFILE%\.m2\repository\ch\qos\logback\logback-core\1.5.6\logback-core-1.5.6.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\ch\qos\logback\logback-core\1.5.6\logback-core-1.5.6.jar
        if exist "%USERPROFILE%\.m2\repository\org\lz4\lz4-java\1.8.0\lz4-java-1.8.0.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\org\lz4\lz4-java\1.8.0\lz4-java-1.8.0.jar
        if exist "%USERPROFILE%\.m2\repository\com\github\luben\zstd-jni\1.5.6-6\zstd-jni-1.5.6-6.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\com\github\luben\zstd-jni\1.5.6-6\zstd-jni-1.5.6-6.jar
        if exist "%USERPROFILE%\.m2\repository\org\xerial\snappy\snappy-java\1.1.10.5\snappy-java-1.1.10.5.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\org\xerial\snappy\snappy-java\1.1.10.5\snappy-java-1.1.10.5.jar
        if exist "%USERPROFILE%\.m2\repository\com\fasterxml\jackson\core\jackson-core\2.17.2\jackson-core-2.17.2.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\com\fasterxml\jackson\core\jackson-core\2.17.2\jackson-core-2.17.2.jar
        if exist "%USERPROFILE%\.m2\repository\com\google\code\gson\gson\2.8.9\gson-2.8.9.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\com\google\code\gson\gson\2.8.9\gson-2.8.9.jar
        if exist "%USERPROFILE%\.m2\repository\org\apache\avro\avro\1.12.0\avro-1.12.0.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\org\apache\avro\avro\1.12.0\avro-1.12.0.jar
        if exist "%USERPROFILE%\.m2\repository\com\google\code\findbugs\jsr305\3.0.2\jsr305-3.0.2.jar" set CLASSPATH=!CLASSPATH!;%USERPROFILE%\.m2\repository\com\google\code\findbugs\jsr305\3.0.2\jsr305-3.0.2.jar
    )
)

if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"

echo Starting DieselDB server on port %PORT% with data dir %DATA_DIR%
java -cp "%CLASSPATH%" diesel.DatabaseServer %PORT% %DATA_DIR%
