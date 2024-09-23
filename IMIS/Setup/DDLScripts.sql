
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
