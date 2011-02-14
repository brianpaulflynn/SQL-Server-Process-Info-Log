truncate table ProcessInfoLog
truncate table LogDateTime

while 1=1 begin
	exec ProcessInfoLog_LogData_sp
	waitfor delay '00:01:00'
end -- while
