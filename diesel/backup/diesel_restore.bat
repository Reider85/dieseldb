@echo off
REM DieselDB Physical Restore Tool
REM Usage: diesel_restore.bat <backup-dir> <target-dir>

if "%1"=="" (
    echo Usage: diesel_restore.bat ^<backup-dir^> ^<target-dir^>
    echo.
    echo Example: diesel_restore.bat C:\path\to\backup C:\path\to\restored_database
    exit /b 1
)

if "%2"=="" (
    echo Usage: diesel_restore.bat ^<backup-dir^> ^<target-dir^>
    echo.
    echo Example: diesel_restore.bat C:\path\to\backup C:\path\to\restored_database
    exit /b 1
)

REM Set classpath - adjust based on your environment
set CLASSPATH=target\classes;lib\*

REM Run the Java application
java -cp "%CLASSPATH%" diesel.backup.DieselRestore restore %1 %2

if %ERRORLEVEL% NEQ 0 (
    echo Restore failed with error code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)

echo Restore completed successfully
exit /b 0