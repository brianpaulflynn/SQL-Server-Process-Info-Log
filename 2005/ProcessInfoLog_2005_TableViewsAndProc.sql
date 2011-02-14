/*
drop table LogDateTime
drop table ProcessInfoLog
drop view ProcessInfoLog_vw
drop proc ProcessInfoLog_LogData_sp
drop view ProcessInfo_cube
*/
go
CREATE TABLE [dbo].[LogDateTime] (
	LogDateTimeID	int identity (-2147483648,1)
,	Yr		smallint
,	Qt		varchar(10)
,	Mo		varchar(10)
,	Wk		tinyint
,	Dy		tinyint
,	Hr		tinyint
,	Mn		tinyint
)
GO
ALTER TABLE dbo.LogDateTime ADD CONSTRAINT
	PK_LogDate PRIMARY KEY CLUSTERED
	(
	LogDateTimeID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
CREATE TABLE [dbo].[ProcessInfoLog] (
	ProcessInfoLogID bigint identity (-9223372036854775808,1)
--,	[LogDate] [datetime] NOT NULL 
,	[LogDateTimeID] [bigint] NOT NULL
,	[ProcessID] [int] NOT NULL 
,	[Host] [varchar] (100) NULL 
,	[Database] [varchar] (100) NOT NULL 
,	[User] [varchar] (50) NOT NULL 
,	[Application] [varchar] (255) NULL 
,	[ContextID] [int] NULL 
,	[LoginTime] [datetime] NOT NULL 
,	[CPU] [int] NULL 
,	[PhysicalIO] [int] NULL
)
GO
ALTER TABLE dbo.ProcessInfoLog ADD CONSTRAINT
	PK_ProcessInfoLog PRIMARY KEY CLUSTERED 
	(
	ProcessInfoLogID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO
create view ProcessInfoLog_vw
as
select	pil.ProcessInfoLogID
,	pil.[LogDateTimeID]
,	pil.[ProcessID]
,	pil.[Host]
,	pil.[Database]
,	pil.[User]
,	pil.[Application]
,	pil.[ContextID]
,	pil.[LoginTime]
,	pil.[CPU]
,	pil.[PhysicalIO]
from	ProcessInfoLog		pil
	inner join LogDateTime	ldt	on	ldt.LogDateTimeID	=	pil.LogDateTimeID
go
create proc ProcessInfoLog_LogData_sp
as
declare	@LogDate	smalldatetime
,	@LogDateTimeID	int
select	@LogDate	= getdate()

insert	LogDateTime ( Yr , Qt , Mo , Wk , Dy , Hr , Mn )
select	datepart( yyyy , @LogDate )
,	'Quarter ' + convert( char(1) , datepart( qq , @LogDate ) )
,	case	datepart( mm , @LogDate )
		when	1	then	'January'
		when	2	then	'February'
		when	3	then	'March'
		when	4	then	'April'
		when	5	then	'May'
		when	6	then	'June'
		when	7	then	'July'
		when	8	then	'August'
		when	9	then	'September'
		when	10	then	'October'
		when	11	then	'November'
		when	12	then	'December'
		else			'???'
	end
,	datepart( wk , @LogDate )
,	datepart( dd , @LogDate )
,	datepart( hh , @LogDate )
,	datepart( mi , @LogDate )
select	@LogDateTimeID = Scope_Identity()
				
insert	ProcessInfoLog (
	[LogDateTimeID]
,	[ProcessID]
,	[Host]
,	[Database]
,	[User]
,	[Application]
,	[ContextID]
,	[LoginTime]
,	[CPU]
,	[PhysicalIO]
)
select	@LogDateTimeID -- getdate()
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
go
CREATE view dbo.ProcessInfo_cube
as
select	ProcessInfoLogID
,	LogDateTimeID
,	[LoginTime]
,	[ProcessID]
,	ContextID
,	convert( varchar , [LoginTime] , 126 ) + ' - ' + convert( varchar , [ProcessID] )  + ' - ' + convert( varchar , ContextID )	as Session
,	[Database]
,	Host
,	Application
,	CPU
,	case 	when		(	select	top 1 CPU
					from	ProcessInfoLog_vw
					where	[LoginTime]	=	i.[LoginTime]
					and	[ProcessID]	=	i.[ProcessID]
					and	ContextID	=	i.ContextID
					group by
						CPU , LogDateTimeID
					having	LogDateTimeID	< i.LogDateTimeID
					order by
						LogDateTimeID desc
				)	is	null	then	
--			0
			case	when	(	select	top 1 [LoginTime]
						from	ProcessInfoLog_vw
						where	[LoginTime]	=	i.[LoginTime]
						and	[ProcessID]	=	i.[ProcessID]
						and	ContextID	=	i.ContextID
						group by
							[LoginTime] , LogDateTimeID
						having	LogDateTimeID	< i.LogDateTimeID
						order by
							LogDateTimeID desc
					)
					>
					(	select	top 1 LogDateTimeID
						from	ProcessInfoLog_vw
						where	[LoginTime]	=	i.[LoginTime]
						and	[ProcessID]	=	i.[ProcessID]
						and	ContextID	=	i.ContextID
						group by
							LogDateTimeID
						having	LogDateTimeID	< i.LogDateTimeID
						order by
							LogDateTimeID desc
					)
					then	CPU
				else		0
			end
		else	
			case	when	CPU	>	(	select	top 1 CPU
								from	ProcessInfoLog_vw
								where	[LoginTime]	=	i.[LoginTime]
								and	[ProcessID]	=	i.[ProcessID]
								and	ContextID	=	i.ContextID
								group by
									CPU , LogDateTimeID
								having	LogDateTimeID	< i.LogDateTimeID
								order by
									LogDateTimeID desc
							)
					then
						CPU - 	(	select	top 1 CPU
								from	ProcessInfoLog_vw
								where	[LoginTime]	=	i.[LoginTime]
								and	[ProcessID]	=	i.[ProcessID]
								and	ContextID	=	i.ContextID
								group by
									CPU , LogDateTimeID
								having	LogDateTimeID	< i.LogDateTimeID
								order by
									LogDateTimeID desc
							)
					else	0
			end
	end
		as CPU_change
,	[PhysicalIO]
,	case 	when				(	select	top 1 [PhysicalIO]
							from	ProcessInfoLog_vw
							where	[LoginTime]	=	i.[LoginTime]
							and	[ProcessID]	=	i.[ProcessID]
							and	ContextID	=	i.ContextID
							group by
								[PhysicalIO] , LogDateTimeID
							having	LogDateTimeID	< i.LogDateTimeID
							order by
								LogDateTimeID desc
						)	is	null	then	
			0
		else	
			[PhysicalIO]	 - 	(	select	top 1 [PhysicalIO]
							from	ProcessInfoLog_vw
							where	[LoginTime]	=	i.[LoginTime]
							and	[ProcessID]	=	i.[ProcessID]
							and	ContextID	=	i.ContextID
							group by
								[PhysicalIO] , LogDateTimeID
							having	LogDateTimeID	< i.LogDateTimeID
							order by
								LogDateTimeID desc
						)
	end
		as PIO_change
,	[User]
from	ProcessInfoLog_vw		i
go
