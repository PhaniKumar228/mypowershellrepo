FUNCTION Run-SQL 
{
    PARAM(
            [string] $dataSource = "localhost",
            [string] $database = "Master",
            [string] $sqlCommand = $(throw "Please specify a query.")
          )

    $connectionString = "Data Source=$dataSource; " + "Integrated Security=SSPI; " + "Initial Catalog=$database" + "; max pool size=500"

    $connection = new-object system.data.SqlClient.SQLConnection($connectionString)
    $command = new-object system.data.sqlclient.sqlcommand($sqlCommand,$connection)
    $connection.Open()

    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataSet) | Out-Null

    $connection.Close()
    $dataSet.Tables
}


CLS

$command = "select LTrim(RTrim(ServerName)) AS ServerName from eMDW..[DB_ServerGroup] where GroupName  in ( 'PRD', 'DRS') AND ServerName not like '%HA34%' -- populated by EMDW_LoadServerData.ps1"
#$command = "select LTrim(RTrim(ServerName)) AS ServerName from eMDW..[DB_ServerGroup] where ServerName in ( 'MARSQLPRD21A', 'MARPRDSQL073','MARSQLPRD80A','MARSQLPRD94','MARSQLPRD92','MARSQLDRS87','MARSQLPRD88','PRDSQL100-A','PRDSQL100-B\inst2') --'MARSQLPRD34' "
$Servers = Run-SQL -dataSource eMDWPROD -database eMDW -sqlCommand $command 



    
FOREACH($sqlName in $Servers)
{ 
        
        $Server = $sqlName.ServerName

        # Get Computer/host name
        $Server1 = $Server.Replace("\INST2","") 
        $Server1 = $Server1.Replace("\inst2","") 

    write-host "Connecting to server $Server1" -ForegroundColor Yellow
    IF(Test-Connection $Server1 -Quiet -Count 1)
    {
        try
        {
            # Query to run tlog backup : will create one TLOG backup job per qualifying database and run them – all at the same time. In an always on config, it will run against the primary only. This won’t run against GDM.
        $query = "SET NOCOUNT ON;"
        #$query = $query + "`n" +" EXEC [gmodba].[dbo].[usp__Backup_RunTLOGBackups] @RunAll = 1, @OneOff = 1 "
        $query = $query + "`n" +" select name from sys.server_principals where name like '%Service-SQLCLPRD%' --name like '%Service-GPReporter%' or name like '%Service-SQLMaint%' or name like '%service-rpsql%' or name like '%SQL Services - Research - Prod%' or name like '%Service-HDS%' or name like '%Service-GPReporter%' or name like '%Service-CSpSQL%' or name like '%ITDBAAdmins%'  "
        #write-host "Run query: $query " -ForegroundColor Green
            # Run the query
        Run-SQL -dataSource $Server -database master -sqlCommand $query 
        }
        CATCH
        {
            write-Host "Server $Server Error" 
        }
     }  
        else
        
        {
            write-Host "Server $Server Error 2" 
        }
}