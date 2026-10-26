@echo off
REM DieselDB Physical Backup Tool
REM Usage: diesel_backup.bat <database-dir> <backup-dir>

if "%1"=="" (
    echo Usage: diesel_backup.bat ^<database-dir^> ^<backup-dir^>
    echo.
    echo Example: diesel_backup.bat C:\path\to\database C:\path\to\backup
    exit /b 1
)

if "%2"=="" (
    echo Usage: diesel_backup.bat ^<database-dir^> ^<backup-dir^>
    echo.
    echo Example: diesel_backup.bat C:\path\to\database C:\path\to\backup
    exit /b 1
)

REM Set classpath - adjust based on your environment
set CLASSPATH=target\classes;lib\*

REM Run the Java application
java -cp "%CLASSPATH%" diesel.backup.DieselBackup backup %1 %2

if %ERRORLEVEL% NEQ 0 (
    echo Backup failed with error code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)

echo Backup completed successfully
exit /b 0