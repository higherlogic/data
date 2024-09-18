param (
    [Parameter(Mandatory=$true, HelpMessage="The DB server that will host the meta data database")]
    [string]$CentralDBServer,
    [Parameter(Mandatory=$true, HelpMessage="The DB server login name")]
    [string]$CentralDBLogin,
    [Parameter(Mandatory=$true, HelpMessage="The DB server password")]
    [string]$CentralDBPwd
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
   $SQLSetup = Invoke-Sqlcmd -ServerInstance $CentralDBServer -Database DataExport -Query $sql -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd
}

# Rest of your code goes here...
