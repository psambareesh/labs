USE [RBAC]
GO
/****** Object:  Table [dbo].[Environment]    Script Date: 8/30/2025 4:02:59 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Environment](
	[EnvironmentCode] [varchar](5) NOT NULL,
	[EnvironmentName] [varchar](50) NULL,
 CONSTRAINT [PK_Environment] PRIMARY KEY CLUSTERED 
(
	[EnvironmentCode] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PrincipalMaster]    Script Date: 8/30/2025 4:02:59 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PrincipalMaster](
	[principal_id] [varchar](128) NOT NULL,
	[source_system_id] [varchar](50) NOT NULL,
	[EnvironmentCode] [varchar](5) NOT NULL,
	[principal_type] [varchar](50) NULL,
	[display_name] [varchar](256) NULL,
	[principal_internal_id] [varchar](128) NULL,
	[email] [varchar](256) NULL,
	[creation_date] [datetime] NULL,
	[last_access_date] [datetime] NULL,
	[jira_ticket] [varchar](128) NULL,
	[currentstatus] [int] NULL,
 CONSTRAINT [PK_PrincipalMaster] PRIMARY KEY CLUSTERED 
(
	[principal_id] ASC,
	[source_system_id] ASC,
	[EnvironmentCode] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PrincipalType]    Script Date: 8/30/2025 4:02:59 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PrincipalType](
	[principal_type] [varchar](50) NOT NULL,
	[decription] [varchar](100) NULL,
 CONSTRAINT [PK_PrincipalType] PRIMARY KEY CLUSTERED 
(
	[principal_type] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[RBACMatrix]    Script Date: 8/30/2025 4:02:59 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[RBACMatrix](
	[RBACMatrix_id] [int] IDENTITY(1,1) NOT NULL,
	[run_id] [int] NULL,
	[principal_type] [varchar](50) NULL,
	[principal_id] [varchar](128) NULL,
	[source_system_id] [varchar](50) NULL,
	[EnvironmentCode] [varchar](5) NULL,
	[service] [varchar](100) NULL,
	[access_level] [varchar](100) NULL,
	[last_updated] [datetime] NULL,
 CONSTRAINT [PK_RBACMatrix] PRIMARY KEY CLUSTERED 
(
	[RBACMatrix_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[RunHistory]    Script Date: 8/30/2025 4:02:59 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[RunHistory](
	[run_id] [int] IDENTITY(1,1) NOT NULL,
	[run_date] [datetime] NULL,
	[triggered_by] [varchar](250) NULL,
	[description] [varchar](500) NULL,
 CONSTRAINT [PK_RunHistory] PRIMARY KEY CLUSTERED 
(
	[run_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[SourceSystem]    Script Date: 8/30/2025 4:02:59 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[SourceSystem](
	[source_system_id] [varchar](50) NOT NULL,
	[source_system_name] [varchar](100) NULL,
 CONSTRAINT [PK_SourceSystem] PRIMARY KEY CLUSTERED 
(
	[source_system_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[PrincipalMaster]  WITH CHECK ADD  CONSTRAINT [FK_PrincipalMaster_Environment] FOREIGN KEY([EnvironmentCode])
REFERENCES [dbo].[Environment] ([EnvironmentCode])
GO
ALTER TABLE [dbo].[PrincipalMaster] CHECK CONSTRAINT [FK_PrincipalMaster_Environment]
GO
ALTER TABLE [dbo].[PrincipalMaster]  WITH CHECK ADD  CONSTRAINT [FK_PrincipalMaster_PrincipalType] FOREIGN KEY([principal_type])
REFERENCES [dbo].[PrincipalType] ([principal_type])
GO
ALTER TABLE [dbo].[PrincipalMaster] CHECK CONSTRAINT [FK_PrincipalMaster_PrincipalType]
GO
ALTER TABLE [dbo].[PrincipalMaster]  WITH CHECK ADD  CONSTRAINT [FK_PrincipalMaster_SourceSystem] FOREIGN KEY([source_system_id])
REFERENCES [dbo].[SourceSystem] ([source_system_id])
GO
ALTER TABLE [dbo].[PrincipalMaster] CHECK CONSTRAINT [FK_PrincipalMaster_SourceSystem]
GO
ALTER TABLE [dbo].[RBACMatrix]  WITH CHECK ADD  CONSTRAINT [FK_RBACMatrix_Environment] FOREIGN KEY([EnvironmentCode])
REFERENCES [dbo].[Environment] ([EnvironmentCode])
GO
ALTER TABLE [dbo].[RBACMatrix] CHECK CONSTRAINT [FK_RBACMatrix_Environment]
GO
ALTER TABLE [dbo].[RBACMatrix]  WITH CHECK ADD  CONSTRAINT [FK_RBACMatrix_PrincipalType] FOREIGN KEY([principal_type])
REFERENCES [dbo].[PrincipalType] ([principal_type])
GO
ALTER TABLE [dbo].[RBACMatrix] CHECK CONSTRAINT [FK_RBACMatrix_PrincipalType]
GO
ALTER TABLE [dbo].[RBACMatrix]  WITH CHECK ADD  CONSTRAINT [FK_RBACMatrix_RunHistory] FOREIGN KEY([run_id])
REFERENCES [dbo].[RunHistory] ([run_id])
GO
ALTER TABLE [dbo].[RBACMatrix] CHECK CONSTRAINT [FK_RBACMatrix_RunHistory]
GO
ALTER TABLE [dbo].[RBACMatrix]  WITH CHECK ADD  CONSTRAINT [FK_RBACMatrix_SourceSystem] FOREIGN KEY([source_system_id])
REFERENCES [dbo].[SourceSystem] ([source_system_id])
GO
ALTER TABLE [dbo].[RBACMatrix] CHECK CONSTRAINT [FK_RBACMatrix_SourceSystem]
GO
