param (
    [Parameter(Mandatory=$true, HelpMessage="The Azure Key Vault name")]
        [string] $AzureKeyVaultName,
    [Parameter(Mandatory=$true, HelpMessage="The Azure SQL Database credentials secret name")]    
        [string] $AzureCentralDBCredsSecretName
)
# retrieve DB credentials from Azure Key Vault
$secret = Get-AzKeyVaultSecret -VaultName "$AzureKeyVaultName" -Name "$AzureCentralDBCredsSecretName" -AsPlainText
$properties = $secret -split "`n" | ForEach-Object {
    $key, $value = $_ -split ":", 2
    $key.Trim(), $value.Trim()
}
$propertiesHashTable = @{}
for ($i = 0; $i -lt $properties.Length; $i += 2) {
    $propertiesHashTable[$properties[$i]] = $properties[$i + 1]
}
# Access individual properties
$CentralDBServer = $propertiesHashTable["Host"]
$CentralDBLogin = $propertiesHashTable["User"]
$CentralDBPwd = $propertiesHashTable["Password"]


#get pending tenants
$pendingTenants = @"
SELECT TenantCode, ServerName, DBName
FROM    [ChangeTrackingTenants]
WHERE [Status] = 'Pending'
ORDER BY TenantCode
"@

$tenants = Invoke-Sqlcmd -ServerInstance $CentralDBServer -Database DataExport -Query $pendingTenants -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd

foreach($tenant in $tenants)
{
try {
    
    $tenantCode = $tenant.TenantCode
    $serverName = $tenant.ServerName
    $dbName = $tenant.DBName

    #enable change tracking on the tenant database
    $enableChangeTracking = @"
    IF NOT EXISTS (
		SELECT  *
		FROM    	master.sys.change_tracking_databases 
		where db_name(database_id) = '$dbName'
	)
		ALTER DATABASE [$dbName] SET CHANGE_TRACKING = ON  (CHANGE_RETENTION = 10 DAYS, AUTO_CLEANUP = ON)
"@
        
        Invoke-Sqlcmd -ServerInstance $serverName -Database master -Query $enableChangeTracking -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd -QueryTimeout 0 -ErrorAction Stop
    
        #create tables/SPs in the tenant database
        $ddlsql = Get-Content -Raw -Path "$PSScriptRoot\DDLScripts-TenantDatabase.sql"
        $ddlsql = $ddlsql -replace "REPLACE_WITH_TENANT_CODE", $tenantCode


        $sqlstatements = $ddlsql -split "(?m)^\s*GO\s*$", 0, "multiline"
        foreach($sql in $sqlstatements)
        {
         #   Write-Host $sql
           Invoke-Sqlcmd -ServerInstance $serverName -Database $dbName -Query $sql -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd
        }

    #enable change tracking on the tenant tables
    $tableSQL = @"
    SELECT SchemaName, TableName
    FROM dbo.ChangeTrackingTablesToExport
    WHERE TenantCode = '$tenantCode'
    AND EnabledForExport = 'pending'
"@
    $tables = Invoke-Sqlcmd -ServerInstance  $serverName -Database $dbName -Query $tableSQL -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd

    foreach($table in $tables)
    {
        $schemaName = $table.SchemaName
        $tableName = $table.TableName
        

        $enableTableChangeTracking = @"
        IF EXISTS (SELECT  * FROM sys.tables WHERE name = '$tableName' AND schema_id = SCHEMA_ID('$schemaName'))
			ALTER TABLE [$schemaName].[$tableName] ENABLE CHANGE_TRACKING 
"@
        
        Invoke-Sqlcmd -ServerInstance $serverName -Database $dbName -Query $enableTableChangeTracking -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd -QueryTimeout 0 -ErrorAction Stop

        $updateTableStatus = @"
        UPDATE [ChangeTrackingTablesToExport]
        SET [EnabledForExport] = 'Enabled'
        WHERE TenantCode = '$tenantCode'
        AND SchemaName = '$schemaName'
        AND TableName = '$tableName'      
"@
        Invoke-Sqlcmd -ServerInstance $serverName -Database $dbName -Query $updateTableStatus -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd -QueryTimeout 0 -ErrorAction Stop
    }


        $updateTenantStatus = @"
        UPDATE [ChangeTrackingTenants]
        SET [Status] = 'Enabled'
        WHERE TenantCode = '$tenantCode'        
"@        
        Invoke-Sqlcmd -ServerInstance $CentralDBServer -Database DataExport -Query $updateTenantStatus -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd -QueryTimeout 0 -ErrorAction Stop


}
catch {
    Write-Log -errorLevel ERROR -message "Tenant: $tenantCode | Server: $serverName | DB: $dbName"
    Write-Log -errorLevel ERROR -message $_.Exception.Message -stack $_.Exception.StackTrace
    Write-log -errorLevel ERROR -message $_.Exception.InnerException
    throw

}    

}

# get all enabled tenants and enabled tables that are pending; this can be from new tables that are added for the tenant or tables that were missed in the initial setup
$enabledTenantsSQL = @"
    SELECT TenantCode, ServerName, DBName
    FROM    [ChangeTrackingTenants]
    WHERE [Status] = 'Enabled'
    ORDER BY TenantCode
"@
    
$enabledTenants = Invoke-Sqlcmd -ServerInstance $CentralDBServer -Database DataExport -Query $enabledTenantsSQL -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd

foreach($tenant in $enabledTenants)
{
    $tenantCode = $tenant.TenantCode
    $serverName = $tenant.ServerName
    $dbName = $tenant.DBName

    $tableSQL = @"
    SELECT SchemaName, TableName
    FROM ChangeTrackingTablesToExport
    WHERE TenantCode = '$tenantCode'
    AND EnabledForExport = 'Pending'
"@
    $tables = Invoke-Sqlcmd -ServerInstance  $serverName -Database $dbName -Query $tableSQL -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd

    foreach($table in $tables)
    {
        $schemaName = $table.SchemaName
        $tableName = $table.TableName
        

        $enableTableChangeTracking = @"
        IF EXISTS (SELECT  * FROM sys.tables WHERE name = '$tableName' AND schema_id = SCHEMA_ID('$schemaName'))
            ALTER TABLE [$schemaName].[$tableName] ENABLE CHANGE_TRACKING
"@
            
            Invoke-Sqlcmd -ServerInstance $serverName -Database $dbName -Query $enableTableChangeTracking -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd -QueryTimeout 0 -ErrorAction Stop
    
            $updateTableStatus = @"
            UPDATE [ChangeTrackingTablesToExport]
            SET [EnabledForExport] = 'Enabled'
            WHERE TenantCode = '$tenantCode'
            AND SchemaName = '$schemaName'
            AND TableName = '$tableName'            
"@
            Invoke-Sqlcmd -ServerInstance $serverName -Database $dbName -Query $updateTableStatus -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd -QueryTimeout 0 -ErrorAction Stop
    
    
            }
}
