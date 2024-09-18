
-- tables without a primary key
select schema_name(t.schema_id) as [schema_name], t.[name] as table_name
from sys.tables t
    left outer join sys.indexes pk
        on t.object_id = pk.object_id 
        and pk.is_primary_key = 1
where pk.object_id is null
order by schema_name(t.schema_id), t.[name]