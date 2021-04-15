PARAM
(
[STRING]$ServerName,
$SendEmail = 1
)

# ----------------------------------------------------------------- #
# Function : RUN-SQL                                                #
# ----------------------------------------------------------------- #
FUNCTION Run-SQL {
    PARAM(
            [string] $dataSource = "localhost",
            [string] $database = "Master",
            [string] $sqlCommand = $(throw "Please specify a query.")
          )

    $connectionString = "Data Source=$dataSource; " + "Integrated Security=SSPI; " + "Initial Catalog=$database"

    $connection = new-object system.data.SqlClient.SQLConnection($connectionString)
    $command = new-object system.data.sqlclient.sqlcommand($sqlCommand,$connection)
    $connection.Open()

    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataSet) | Out-Null

    $connection.Close()
    $dataSet.Tables

}


$SQLCMD = @"
DECLARE @isKerberos           INT

SELECT @isKerberos = count(auth_scheme) FROM sys.dm_exec_connections (nolock) WHERE session_id = @@spid and auth_scheme = 'KERBEROS'
 
IF (@isKerberos = 0)
BEGIN
	DECLARE @EmailRecipient VARCHAR(1000), @emailSub NVARCHAR(256),@ProfileName VARCHAR(1000),@tableHTML VARCHAR(MAX),@EmailRecipientCC  VARCHAR(1000)
	SET @emailSub = '$ServerName Kerberos Issue!!'
	SELECT TOP 1 @ProfileName = name FROM msdb.dbo.sysmail_profile
	set @EmailRecipient  = 'ITDBA@gmo.com'
	SET @tableHTML = 'Looks like the SPNs did not get registered properly..
	
	<br>
    <br>
	Run recycle the SQL serices. If that does not help run the below commands to fix it. 
	<br>
	setspn –s MSSQLSvc/$ServerName.GMO.TLD GMO\SERVICE-PSQL
	<br>
    setspn –s MSSQLSvc/$ServerName.GMO.TLD:1433 GMO\SERVICE-PSQL
	<br>
	' 
"@
IF ($SendEmail = 1)
{
$SQLCMD = $SQLCMD + @"
EXEC msdb.dbo.sp_send_dbmail
	@profile_name = @ProfileName,
	@recipients=@EmailRecipient,
	@body=@tableHTML,
	@body_format = 'HTML',
	@subject =@emailSub
"@
}

$SQLCMD = $SQLCMD + @"

end

select @isKerberos

"@

$k = Run-SQL -datasource $ServerName -database "GMODBA" -sqlCommand $SQLCMD

IF ($K) 
{
    write-host "Kerberos is working"
}
ELSE
{
    WRITE-HOST "Kerberos isn't working"
}
