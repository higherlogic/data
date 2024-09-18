

insert into [dbo].[ChangeTrackingTenants](TenantCode, DBName, ServerName)
values('Client1', 'Main20_3_180_315_aioCopy', 'higherlogic-asi-dev.database.windows.net')


insert into [dbo].[ChangeTrackingTablesToExport](TenantCode, SchemaName, TableName, PrimaryKey)
values('Client1', 'dbo', 'ContactAddress', 'ContactAddressKey')
, ('Client1', 'dbo', 'ContactSalutation', 'ContactSalutationKey')
, ('Client1', 'dbo', 'ContactMain', 'ContactKey')
,('Client1', 'dbo', 'DocumentMain', 'DocumentKey')


