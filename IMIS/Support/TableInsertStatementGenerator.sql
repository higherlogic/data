-- select statement to generate the insert statement for tables that should be exported
-- modify as needed
SELECT 	'INSERT INTO [dbo].[ChangeTrackingTablesToExport](TenantCode, SchemaName, TableName, PrimaryKey)' as sqlstatement
UNION
SELECT sqlstatement
FROM (

select top (100) percent
'(''REPLACE_WITH_TENANT_CODE'', ''' + schema_name(t.schema_id) + ''', ''' + t.[name] + ''', '''  + pk.name + '''),' as sqlstatement
from sys.tables t
    join sys.indexes pk
        on t.object_id = pk.object_id 
        and pk.is_primary_key = 1
where t.[name] NOT IN ('ContactAddress','ContactMain','ContactSalutation','DocumentMain')
order by schema_name(t.schema_id), t.[name]
) t