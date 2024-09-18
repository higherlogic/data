
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ChangeTrackingTenants](
	[TenantCode] [varchar](100) NOT NULL,
	[DBName] [varchar](100) NOT NULL,
	[ServerName] [varchar](100) NOT NULL,
	[Status] [varchar](200) NULL,
	[DateAdded] [smalldatetime] NULL,
 CONSTRAINT [PK_ChangeTrackingTenants] PRIMARY KEY CLUSTERED 
(
	[TenantCode] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


ALTER TABLE [dbo].[ChangeTrackingTenants] ADD CONSTRAINT [DF_ChangeTrackingTenants_Status]   DEFAULT ('Pending') FOR [Status]
ALTER TABLE [dbo].[ChangeTrackingTenants] ADD CONSTRAINT [DF_ChangeTrackingTenants_DateAdded] DEFAULT (getdate()) FOR [DateAdded]
GO

CREATE TABLE [dbo].[ChangeTrackingTablesToExport](
	[Id] bigint NOT NULL IDENTITY,
	[TenantCode] [varchar](100) NOT NULL,
	[SchemaName] [varchar](100) NOT NULL,
	[TableName] [varchar](100) NOT NULL,
	[PrimaryKey] [varchar](1000) NOT NULL,
	[DateAdded] [smalldatetime] NULL,
	[EnabledForExport] [varchar](20) NOT NULL,
 CONSTRAINT [PK_ChangeTrackingTablesToExport] PRIMARY KEY CLUSTERED 
(
	ID ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER AUTHORIZATION ON [dbo].[ChangeTrackingTablesToExport] TO  SCHEMA OWNER 

ALTER TABLE [dbo].[ChangeTrackingTablesToExport] ADD  CONSTRAINT [DF_ChangeTrackingTablesToExport_SchemaName]  DEFAULT ('dbo') FOR [SchemaName]


ALTER TABLE [dbo].[ChangeTrackingTablesToExport] ADD  CONSTRAINT [DF_ChangeTrackingTablesToExport_DateAdded]  DEFAULT (getdate()) FOR [DateAdded]

ALTER TABLE [dbo].[ChangeTrackingTablesToExport] ADD  CONSTRAINT [DF_ChangeTrackingTablesToExport_EnabledForExport]  DEFAULT ('pending') FOR [EnabledForExport]
	 
ALTER TABLE [dbo].[ChangeTrackingTablesToExport]
ADD CONSTRAINT FK_ChangeTrackingTablesToExport_TenantCode FOREIGN KEY (TenantCode) 
REFERENCES [dbo].[ChangeTrackingTenants]([TenantCode])
	ON DELETE CASCADE
	ON UPDATE CASCADE;

GO


CREATE TABLE [dbo].[ChangeTrackingVersion](
	[Id] bigint NOT NULL IDENTITY,
	[TablesToExportId] bigint NOT NULL,
	[LastSuccessfulRunVersionId] [bigint] NOT NULL,
	[LastExportDate] [datetime] NOT NULL,
 CONSTRAINT [PK_ChangeTrackingVersion] PRIMARY KEY CLUSTERED 
(
	Id ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[ChangeTrackingVersion] ADD  CONSTRAINT [DF_LastExportDate]  DEFAULT (getdate()) FOR [LastExportDate]

ALTER TABLE [dbo].[ChangeTrackingVersion]
ADD CONSTRAINT FK_ChangeTrackingVersion_TablesToExportId FOREIGN KEY (TablesToExportId) 
REFERENCES [dbo].[ChangeTrackingTablesToExport]([Id])
	ON DELETE CASCADE
	ON UPDATE CASCADE;



GO


CREATE OR ALTER PROC [dbo].[ChangeTracking_GetChangedTables]
@tenantCode varchar(100)
AS

-- this script gets a list of tables that have changed data since the last export
-- it also incudes tables for which changes have never been exported

DECLARE @CurrentVersion BIGINT, @sqlVersion nvarchar(max)
SET @sqlVersion = 'USE [' + @tenantCode + '];    SELECT @ReturnValue = CHANGE_TRACKING_CURRENT_VERSION()'
EXEC sys.sp_executesql @sqlVersion,  N'@ReturnValue BIGINT OUTPUT', @ReturnValue = @CurrentVersion OUTPUT
DROP TABLE IF EXISTS #tablesWithChanges
CREATE TABLE #tablesWithChanges(SchemaName varchar(100), TableName varchar(100), PrimaryKey varchar(1000), LastSuccessfulRunVersionId bigint)


DECLARE @SQL NVARCHAR(MAX) = N'INSERT INTO #tablesWithChanges(SchemaName, TableName, PrimaryKey, LastSuccessfulRunVersionId)'



	SELECT @SQL = @SQL + CONCAT('SELECT distinct ', '''' + t.SchemaName + ''' as SchemaName,', '''' + t.TableName + ''' as TableName,'
	, '''' + t.PrimaryKey + ''' as PrimaryKey,'
	, LastSuccessfulRunVersionId
	,'as LastSuccessfulRunVersionId	FROM    CHANGETABLE(CHANGES [', tt.DBName, '].[', t.SchemaName + '].[', t.TableName, '] ,', LastSuccessfulRunVersionId,') AS CT 
				 WHERE (SELECT MAX(v) FROM (VALUES(ct.SYS_CHANGE_VERSION),(ct.SYS_CHANGE_CREATION_VERSION)) as VALUE(v) ) <=', @CurrentVersion,
				' 
				UNION ALL ')
				
	FROM DataExport.dbo.ChangeTrackingVersion v
	JOIN DataExport.dbo.ChangeTrackingTablesToExport t ON t.Id = v.TablesToExportId AND t.ExportStatus = 'enabled'
	JOIN DataExport.dbo.[ChangeTrackingTenants] tt ON tt.TenantCode = t.TenantCode
	WHERE tt.TenantCode = @tenantCode
	
	SET @SQL = @SQL +  '
	SELECT  t.SchemaName, t.TableName, t.PrimaryKey, 0 as LastSuccessfulRunVersionId
	FROM    DataExport.dbo.ChangeTrackingVersion v
	RIGHT JOIN DataExport.dbo.ChangeTrackingTablesToExport t ON t.Id = v.TablesToExportId 
	JOIN DataExport.dbo.[ChangeTrackingTenants] tt ON tt.TenantCode = t.TenantCode AND tt.TenantCode = @tenantCode
	WHERE v.LastSuccessfulRunVersionId IS NULL
	'


    EXEC sp_ExecuteSQL @SQL, N'@tenantCode varchar(100)', @tenantCode = @tenantCode


	declare @finalTableSQL nvarchar(max)
	SET @finalTableSQL = 'SELECT t.*
	                      FROM    #tablesWithChanges t JOIN [' + @tenantCode + '].sys.tables st
						  ON t.TableName = st.name AND t.SchemaName = schema_name(st.schema_id)  
						  ORDER BY t.SchemaName, t.TableName '
	
	EXEC sp_ExecuteSQL @finalTableSQL


