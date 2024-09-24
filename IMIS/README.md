# `data-export` Codebase

Instructions to setup an export from a SQL Server hosted database.

## Initial Environment Setup

- Create a new SQL Database "DataExport".
- Create a sql login and map the login to a sql user in the new database "DataExport".
- As of now this same sql login needs to also be mapped to each of the tenant databases as a sql user.
- Create an Azure Key Vault (or use an existing)
- Create a new multi-line secret. 
  - Powershell from Azure shell can be used to create the new secret as follows:
```
$vaultName = "<KeyVaultName>"
$secretName = "<SecretName>"
$secretValue = @"
Host: higherlogic-asi-dev.database.windows.net
Port: 1433
User: dblogin
Password: *********
"@

Set-AzKeyVaultSecret -VaultName $vaultName -Name $secretName -SecretValue (ConvertTo-SecureString -String $secretValue -AsPlainText -Force)
```
- Setup a windows virtual server. Optionally, add a non-OS volume to store setup scripts and exported files. If separate volume not added, the OS volume will be used to store the files and may be lost in event of instance termination.
- In the virtual machine "Access Control" (IAM) add a "role assignment"  
  - Ensure Managed Identity is Enabled:
	- Go to the Azure portal.
	- Navigate to your VM.
	- Under the "Settings" section, select "Identity".
	- Ensure the "System assigned" managed identity is turned on.
  - Assign Key Vault RBAC Roles:
	- Go to your Key Vault in the Azure portal.
	- Under "Access control (IAM)", click on "+ Add role assignment".
	- Select the role "Key Vault Secrets User".
	- In the "Members" section, select "Managed identity" and then choose your VM.
	- Save the changes.
  - Test Secret Access From VM:
	- On VM, use following powershell script to test access:
```
Install-Module -Name "Az" -Force -SkipPublisherCheck -Scope AllUsers
Import-Module Az
Connect-AzAccount -Identity
$secret = Get-AzKeyVaultSecret -VaultName "<KeyVaultName>" -Name "<SecretName>" -AsPlainText
```
- [Download](https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell-on-windows?view=powershell-7.4) and install Powershell 7 on virtual server.
- Clone repo or copy files from repo to new instance.



- Edit the /data/iMIS/Setup/DMLScript-AddTenants.sql
  - Replace existing insert statement with a list of tenants to enable the data export for.
- Edit the /data/iMIS/Setup/DDLScripts-TenantDatabase.sql
  - Replace the following sample insert statement with a list of tables that should be exported.
  > insert into [dbo].[ChangeTrackingTablesToExport](TenantCode, SchemaName, TableName, PrimaryKey)
  - insert values for all existing tables are provided in commented section.
- Execute the powershell file /data/iMIS/Setup/EnvironmentSetup.ps1
- This will install needed powershell modules, create necessary tables in the central database and create two Windows Task Scheduler jobs: 
  - "Enable Change Tracking" - this job runs every hour and will enable change tracking for new tenant databases and also enable change tracking for tables.
  - "Export Changed Data"  - this job runs every hour and will export the data for each tenant database configured in central database.

