param (
    [Parameter(Mandatory=$true, HelpMessage="The DB server that will host the meta data database")]
    [string]$CentralDBServer,
    [Parameter(Mandatory=$true, HelpMessage="The DB server login name")]
    [string]$CentralDBLogin,
    [Parameter(Mandatory=$true, HelpMessage="The DB server password")]
    [string]$CentralDBPwd
)

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
    
    #enable change tracking on the tenant tables
    $tableSQL = @"
    SELECT SchemaName, TableName
    FROM ChangeTrackingTablesToExport
    WHERE TenantCode = '$tenantCode'
    AND EnabledForExport = 'pending'
"@
    $tables = Invoke-Sqlcmd -ServerInstance  $CentralDBServer -Database DataExport -Query $tableSQL -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd

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
        Invoke-Sqlcmd -ServerInstance $CentralDBServer -Database DataExport -Query $updateTableStatus -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd -QueryTimeout 0 -ErrorAction Stop
    }


        $updateTenantStatus = @"
        UPDATE [ChangeTrackingTenants]
        SET [Status] = 'Enabled'
        WHERE TenantCode = '$tenantCode'        
"@        
        Invoke-Sqlcmd -ServerInstance $CentralDBServer -Database DataExport -Query $updateTenantStatus -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd -QueryTimeout 0 -ErrorAction Stop
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
    $tables = Invoke-Sqlcmd -ServerInstance  $CentralDBServer -Database DataExport -Query $tableSQL -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd

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
            Invoke-Sqlcmd -ServerInstance $CentralDBServer -Database DataExport -Query $updateTableStatus -TrustServerCertificate -Username $CentralDBLogin -Password $CentralDBPwd -QueryTimeout 0 -ErrorAction Stop
    
    
            }
}
