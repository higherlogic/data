# Define parameters
param(
    [Parameter(Mandatory=$true, HelpMessage="The base folder path where files will be exported")]
        [string]$ExportFolderBasePath, 
    [Parameter(Mandatory=$true, HelpMessage="The DB server that will host the meta data database")]
        [string]$CentralDBServer, 
    [Parameter(Mandatory=$true, HelpMessage="The DB server login name")]
        [string]$CentralDBLogin,
    [Parameter(Mandatory=$true, HelpMessage="The DB server password")]
        [string]$CentralDBPwd,
    [Parameter(Mandatory=$true, HelpMessage="The central database name that stores meta data")]
        [string]$Database,
    [Parameter(Mandatory=$false)][switch]$DebugInfo = $true
)


try{


Import-Module SqlServer
Import-Module Write-Log

# Define the SQL query to get the list of tenants
$tenantSQL = @"
SELECT ct.TenantCode, ct.DBName, ct.ServerName
FROM dbo.ChangeTrackingTenants ct -- JOIN with TenantMetaTable to verify if tenant still exists (in case there are changes tenant may have been deleted or added incorrectly )
WHERE ct.Status = 'Enabled' 
ORDER BY ct.TenantCode
"@

$tenants = Invoke-Sqlcmd -ServerInstance $CentralDBServer -Database $Database -Query $tenantSQL -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd
Write-log -errorLevel INFO -message "tenants to process: $($tenants.count)"
foreach($tenant in $tenants)
{
    $tenantCode = $tenant.TenantCode
    $tenantDBName = $tenant.DBName
    $tenantDBServer = $tenant.ServerName


    $tableSQL = "exec dbo.ChangeTracking_GetChangedTables"
    $qryStartTime = Get-Date
    $tables = Invoke-Sqlcmd -ServerInstance $tenantDBServer -Database $tenantDBName -Query $tableSQL -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd

    $currentTenantVersion = $tables[0].CurrentVersion

    $qryEndTime = Get-Date
    $qryDuration = ($qryEndTime - $qryStartTime).TotalMilliseconds
    if ($DebugInfo){ Write-log -errorLevel INFO -message "Tenant: $tenantCode; GetChangedTables duration: $qryDuration" }

    foreach($table in $tables)
    {
        $tableId = $table.TableId
        $schemaName = $table.SchemaName
        $tableName = $table.TableName
        $primaryKey = $table.PrimaryKey
        $lastRunVId = $table.LastSuccessfulRunVersionId


        if ($DebugInfo){ Write-log -errorLevel INFO -message "tenant: $tenantCode; Table: $tableName"}

        $fileDir = Join-Path -Path $ExportFolderBasePath -ChildPath "$tenantCode\$tableName"
        $filePath = Join-Path -Path $FileDir -ChildPath "$currentTenantVersion.parquet"

        if (!(Test-Path -Path $FileDir)) {
            New-Item -ItemType Directory -Path $FileDir | Out-Null
        }

        if($lastRunVId -is [DBNull] -or $lastRunVId -eq 0)
        {
            $dataSQL = "IF EXISTS (SELECT  * FROM  [$schemaName].[$tableName])  SELECT cast(0 as bigint) as SYS_CHANGE_VERSION, 'I' as SYS_CHANGE_OPERATION, getdate() as sys_export_date, * FROM [$schemaName].[$tableName] WITH(NOLOCK)"
        }
        else
        {
            $dataSQL = @"
                IF EXISTS (SELECT  * FROM  CHANGETABLE(CHANGES [$schemaName].[$tableName], $lastRunVId) as c) 
                SELECT  CT.SYS_CHANGE_VERSION, CT.SYS_CHANGE_OPERATION, getdate() as sys_export_date, t.* FROM    [$schemaName].[$tableName] AS t LEFT OUTER JOIN  CHANGETABLE(CHANGES [$schemaName].[$tableName], $lastRunVId) AS CT ON 
"@
                        
            foreach($pk in $primaryKey.Split(","))
            {
                $dataSQL = $dataSQL + "t.[$pk] = CT.[$pk] AND "
            }
            $dataSQL = $dataSQL + @"
            1=1
            WHERE (SELECT MAX(v) FROM (VALUES(ct.SYS_CHANGE_VERSION),(ct.SYS_CHANGE_CREATION_VERSION)) as VALUE(v) ) <= $currentTenantVersion
"@  
                
        }

        
        $scriptPath = Split-Path -Path $MyInvocation.MyCommand.Path -Parent
        

        try{


            $exportStartTime = Get-Date
            
            

        & "$scriptPath/WriteObjectDataToParquetPSharp.ps1" -ServerName $tenantDBServer -Database $tenantDBName -dbUser $CentralDBLogin -dbPwd $CentralDBPwd -Query $dataSQL -SchemaName $schemaName -TableName $tableName -FilePath $filePath -DebugInfo $DebugInfo
        

            $exportEndTime = Get-Date
            $exportDuration = ($exportEndTime - $exportStartTime).TotalMilliseconds
            if ($DebugInfo){ Write-log -errorLevel INFO -message "Table: $tableName; Export duration: $exportDuration" }

        $updateMetaDataSQL = @"
            MERGE dbo.ChangeTrackingVersion AS target
                USING (SELECT $tableId as TableId, $currentTenantVersion as LastSuccessfulRunVersionId 
            ) AS source
            ON target.TablesToExportId = source.TableId

            WHEN MATCHED 
            THEN UPDATE
            SET target.LastSuccessfulRunVersionId = source.LastSuccessfulRunVersionId, LastExportDate = getdate()

            WHEN NOT MATCHED 
            THEN INSERT (TablesToExportId, LastSuccessfulRunVersionId)
            VALUES (source.TableId, source.LastSuccessfulRunVersionId);
"@

            Invoke-Sqlcmd -ServerInstance $tenantDBServer -Database $tenantDBName -Query $updateMetaDataSQL -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd
        }
        catch
        {
            Write-log -errorLevel ERROR -message "Error: $_.Exception.Message" -stack $_.Exception.StackTrace
            Write-log -errorLevel ERROR -message "Error: $_.Exception.innerException"
        }
    }
}


}
catch
{
    Write-log -errorLevel ERROR -message ($_.Exception | Format-List -Force | Out-String)
    Write-log -errorLevel ERROR -message ($_.InvocationInfo | Format-List -Force | Out-String)
}