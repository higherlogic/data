param (
    [Parameter(Mandatory=$true, HelpMessage="The DB server that will host the meta data database")]
    [string]$CentralDBServer,
    [Parameter(Mandatory=$true, HelpMessage="The DB server login name")]
    [string]$CentralDBLogin,
    [Parameter(Mandatory=$true, HelpMessage="The DB server password")]
    [string]$CentralDBPwd,
    [Parameter(Mandatory=$true, HelpMessage="The folder to export data to")]
    [string]$ExportFolderBasePath
    
)


# Import SQL Server module
if (! (Get-Module -ListAvailable -Name SqlServer)) {
    Write-Host "SqlServer module does not exist"
    Install-Module -Name SqlServer -Force -SkipPublisherCheck
} 

Import-Module SqlServer

if (! (Get-Module -ListAvailable -Name Write-Log)) {
    Write-Host "Write-Log module does not exist"
    Install-Module -Name Write-Log -Force -SkipPublisherCheck
} 


Import-Module Write-Log

Write-log -errorLevel INFO -message "Setup started..."

$CentralDBServerCheck = Invoke-Sqlcmd -ServerInstance $CentralDBServer -Database DataExport -Query "SELECT getdate() as CurrentDate" -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd

if($CentralDBServerCheck.CurrentDate -ne "")
{
    Write-Host "Successfully connected to database server"
}
else
{
    Write-Host "Could not connect to database server"
    exit
}


$ddlsql = Get-Content -Raw -Path "$PSScriptRoot\DDLScripts.sql"


$sqlstatements = $ddlsql -split "(?m)^\s*GO\s*$", 0, "multiline"

foreach($sql in $sqlstatements)
{
 #   Write-Host $sql
   Invoke-Sqlcmd -ServerInstance $CentralDBServer -Database DataExport -Query $sql -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd
}


# create a task scheduler task to enable change tracking for database and tables
$taskTrigger1 = New-ScheduledTaskTrigger -Daily -At 00:05
$taskTrigger2 = New-ScheduledTaskTrigger -Once -At 00:05 `
        -RepetitionInterval (New-TimeSpan -Minutes 60) `
        -RepetitionDuration (New-TimeSpan -Hours 23 -Minutes 55)
$taskTrigger1.Repetition = $taskTrigger2.Repetition
$changeTrackingEnableScriptPath = "$PSScriptRoot\ChangeTrackingEnable.ps1"
$taskActions = (New-ScheduledTaskAction -Execute '"C:\Program Files\PowerShell\7\pwsh.exe"' -Argument "$changeTrackingEnableScriptPath -CentralDBServer $CentralDBServer -CentralDBLogin $CentralDBLogin -CentralDBPwd $CentralDBPwd ")
Register-ScheduledTask -TaskName 'Enable Change Tracking' -Action $taskActions -Trigger $taskTrigger1 -User "SYSTEM" -RunLevel Highest -Description 'Enable change tracking for new tenants and new tables' -Force



# create a task scheduler task to extract data and sync with s3 bucket
$scriptfolder = (get-item $PSScriptRoot ).Parent.FullName


$taskTrigger3 = New-ScheduledTaskTrigger -Daily -At 00:30
$taskTrigger4 = New-ScheduledTaskTrigger -Once -At 00:30 `
        -RepetitionInterval (New-TimeSpan -Minutes 60) `
        -RepetitionDuration (New-TimeSpan -Hours 23 -Minutes 55)
$taskTrigger3.Repetition = $taskTrigger4.Repetition
$changeTrackingExportScriptPath = "$scriptfolder\ExtractChangedData.ps1"
$taskActions = (New-ScheduledTaskAction -Execute '"C:\Program Files\PowerShell\7\pwsh.exe"' -Argument "$changeTrackingExportScriptPath -ExportFolderBasePath $ExportFolderBasePath  -CentralDBServer $CentralDBServer -CentralDBLogin $CentralDBLogin -CentralDBPwd $CentralDBPwd ")
Register-ScheduledTask -TaskName 'Export Changed Data' -Action $taskActions -Trigger $taskTrigger3 -User "SYSTEM" -RunLevel Highest -Description 'Export changed data for all tenants' -Force


# Rest of your code goes here...
