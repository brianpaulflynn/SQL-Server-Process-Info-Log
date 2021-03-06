/****** Object:  Table [dbo].[ProcessInfoLog]    Script Date: 09/10/2007 15:26:38 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[ProcessInfoLog](
	[LogDate] [datetime] NOT NULL,
	[Process ID] [int] NOT NULL,
	[Host] [varchar](100) NULL,
	[Database] [varchar](100) NOT NULL,
	[User] [varchar](50) NOT NULL,
	[ContextID] [int] NULL,
	[Login Time] [datetime] NOT NULL,
	[Open Transactions] [int] NULL,
	[CPU] [int] NULL,
	[Physical IO] [int] NULL,
	[Memory Usage] [int] NULL,
	[Last Batch] [datetime] NULL,
	[Application] [varchar](255) NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
/****** Object:  View [dbo].[ProcessInfo_cube]    Script Date: 09/10/2007 15:33:32 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[ProcessInfo_cube]
as
select
	LogDate, 
	[Login Time], 
	[Process ID], 
	ContextID, 
--	Session ,
	convert( varchar , [Login Time] , 126 ) + ' - ' + convert( varchar , [Process ID] )  + ' - ' + convert( varchar , ContextID ) 
		as Session ,

	[Database], 
	Host, 
	Application, 

	[Last Batch], 

	CPU , 
	case 	when		(	select	top 1 CPU
					from	ProcessInfoLog
					where	--Session = i.Session
						[Login Time]	=	i.[Login Time]
					and	[Process ID]	=	i.[Process ID]
					and	ContextID	=	i.ContextID
					group by
						CPU , LogDate
					having	LogDate	< i.LogDate
					order by
						LogDate desc
				)	is	null	then	
--			0
			case	when	(	select	top 1 [Login Time]
						from	ProcessInfoLog
						where	-- Session = i.Session
							[Login Time]	=	i.[Login Time]
						and	[Process ID]	=	i.[Process ID]
						and	ContextID	=	i.ContextID
						group by
							[Login Time] , LogDate
						having	LogDate	< i.LogDate
						order by
							LogDate desc
					)
					>
					(	select	top 1 LogDate
						from	ProcessInfoLog
						where	--Session = i.Session
							[Login Time]	=	i.[Login Time]
						and	[Process ID]	=	i.[Process ID]
						and	ContextID	=	i.ContextID
						group by
							LogDate
						having	LogDate	< i.LogDate
						order by
							LogDate desc
					)
					then	CPU
				else		0
			end

		else	
			case	when	CPU	>	(	select	top 1 CPU
								from	ProcessInfoLog
								where	-- Session = i.Session
									[Login Time]	=	i.[Login Time]
								and	[Process ID]	=	i.[Process ID]
								and	ContextID	=	i.ContextID
								group by
									CPU , LogDate
								having	LogDate	< i.LogDate
								order by
									LogDate desc
							)
					then
						CPU - 	(	select	top 1 CPU
								from	ProcessInfoLog
								where	-- Session = i.Session
									[Login Time]	=	i.[Login Time]
								and	[Process ID]	=	i.[Process ID]
								and	ContextID	=	i.ContextID
								group by
									CPU , LogDate
								having	LogDate	< i.LogDate
								order by
									LogDate desc
							)
					else	0
			end
	end
		as CPU_change ,

	[Memory Usage], 
	[Open Transactions], 

	[Physical IO], 
	case 	when				(	select	top 1 [Physical IO]
							from	ProcessInfoLog
							where	--Session = i.Session
								[Login Time]	=	i.[Login Time]
							and	[Process ID]	=	i.[Process ID]
							and	ContextID	=	i.ContextID
							group by
								[Physical IO] , LogDate
							having	LogDate	< i.LogDate
							order by
								LogDate desc
						)	is	null	then	
			0
		else	
			[Physical IO]	 - 	(	select	top 1 [Physical IO]
							from	ProcessInfoLog
							where	--Session = i.Session
								[Login Time]	=	i.[Login Time]
							and	[Process ID]	=	i.[Process ID]
							and	ContextID	=	i.ContextID
							group by
								[Physical IO] , LogDate
							having	LogDate	< i.LogDate
							order by
								LogDate desc
						)
	end
		as PIO_change ,

	[User]
from	ProcessInfoLog	i




GO
/****** Object:  View [dbo].[ProcessInfoLog_vw]    Script Date: 09/10/2007 15:33:32 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[ProcessInfoLog_vw] as
select
	Application, 
	ContextID, 
	CPU, 
	[Database], 
	Host, 
	[Last Batch], 
	LogDate, 
	[Login Time], 
	convert( varchar , [Login Time] , 126 ) + ' - ' + convert( varchar , [Process ID] )  + ' - ' + convert( varchar , ContextID ) 
		as Session ,
	[Memory Usage], 
	[Open Transactions], 
	[Physical IO], 
	[Process ID], 
	[User]
from
	ProcessInfoLog




GO
/****** Object:  StoredProcedure [dbo].[ProcessInfoLog_LogData_sp]    Script Date: 09/10/2007 15:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[ProcessInfoLog_LogData_sp] 

AS

--set nocount on
--create table #tmp(id int)
--declare @id int, @datetime datetime
--select @datetime = getdate()
--insert #tmp (id) exec sp_MSset_current_activity @id  -- refresh the current activity data
--select @id = id from #tmp
--drop table #tmp
--
--create table #activity(
--	[Process ID]		int,
--	[User]			varchar(100),
--	[Database]		varchar(100),
--	[Status]			varchar(100),
--	[Open Transactions]	int,
--	Command		varchar(100),
--	Application		varchar(200),
--	[Wait Time]		int,
--	[Wait Type]		varchar(100),
--	[Wait Resource]		varchar(200),
--	CPU			int,
--	[Physical IO]		int,
--	[Memory Usage]		int,
--	[Login Time]		datetime,
--	[Last Batch]		datetime,
--	Host			varchar(100),
--	[Net Library]		varchar(100),
--	[Net Address]		varchar(100),
--	[Blocked By]		int,
--	Blocking		int,
--	[Execution Context ID]	int
--)
--
--insert 
--	#activity 
--(	[Process ID], 
--	[User], 
--	[Database], 
--	[Status], 
--	[Open Transactions],
--	Command, 
--	Application, 
--	[Wait Time], 
--	[Wait Type],
--	[Wait Resource], 
--	CPU, 
--	[Physical IO], 
--	[Memory Usage], 
--	[Login Time], 
--	[Last Batch], 
--	Host, [Net Library], 
--	[Net Address], 
--	[Blocked By], 
--	Blocking, 
--	[Execution Context ID]
--) 	exec('exec sp_MSget_current_activity ' + @id + ',1')
--
--insert
--	ProcessInfoLog
--	(
--	[LogDate],
--	Host ,
--	[Database],
--	[Process ID],
--	[User],
--	ContextID ,
--	[Login Time],
--	[Open Transactions],
--	[CPU],
--	[Physical IO],
--	[Memory Usage],
--	[Last Batch] ,
--	Application
--	)
--select 	distinct
--	@datetime,
--	Host ,
--	[Database], 
--	[Process ID], 
--	[User], 
--	[Execution Context ID] ,
--	[Login Time],
--	[Open Transactions], 
--	[CPU], 
--	[Physical IO], 
--	[Memory Usage],
--	[Last Batch] ,
--	Application
--from 
--	#activity
--
--drop table #activity

insert	ProcessInfoLog (
	[LogDate]
,	[Process ID]
,	[Host]
,	[Database]
,	[User]
,	[Application]
,	[ContextID]
,	[Login Time]
,	[CPU]
,	[Physical IO]
)
select	getdate()
,	p.spid
,	p.hostname
,	case	when p.dbid = 0	then	'no database context'
		else			db_name(p.dbid)
	end
,	case	when p.spid > 6	then	convert(sysname, ISNULL(suser_sname(p.sid), rtrim(p.nt_domain) + '\' + rtrim(p.nt_username)))
		else			'system'
	end
,	p.program_name
,	p.ecid
,	p.login_time
,	p.cpu
,	p.physical_io
from	master.dbo.sysprocesses p with (NOLOCK)
order by 
	p.spid
