FUNCTION Run-SQL {
    PARAM(
            [string] $dataSource = $(throw "Please specify a SQL Instance"),
            [string] $database = $(throw "Please specify a database"),
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
    
    $now = get-date

    set-location 'd:'
}


$computerssql = 
"
DECLARE @cms as Table
(
	ServerName		sysname,
	Environment		varchar(8)
)


;WITH   
servertype --- CTE
AS     (SELECT server_group_id,
               name
        FROM   msdb.dbo.sysmanagement_shared_server_groups_internal
        WHERE  parent_id = 1),
servercat --- CTE
AS     (SELECT ISNULL(a.server_group_id, b.server_group_id) AS server_group_id,
               a.name AS [Application],
               b.name AS [Type]
        FROM   servertype AS b
               LEFT OUTER JOIN
               msdb.dbo.sysmanagement_shared_server_groups_internal AS a
               ON a.parent_id = b.server_group_id
               AND a.parent_id <> 1)

INSERT INTO @CMS
SELECT distinct b.name AS FullName, Environment =
case A.TYPE 
when 'PRD' THEN 'PROD'
WHEN 'DRS' THEN 'PROD'
ELSE 'NON-PROD'
END
FROM   servercat AS a
       INNER JOIN
       msdb.dbo.sysmanagement_shared_registered_servers_internal AS b
       ON a.server_group_id = b.server_group_id
WHERE
	 b.server_name NOT IN (SELECT ServerName collate SQL_Latin1_General_CP1_CS_AS from [GMODBA].[dbo].[tbl_IDRCMS_compare_ServerExclusionList])
	 and a.type not in ('CLR')

	 SELECT distinct replace(servername,'\inst2','') as ServerName, Environment FROM @CMS
"

$computers = run-sql -datasource emdwprod -database emdw -sqlCommand $computerssql

$Collected = get-date

 $Start = Get-Date
 foreach ($Computer in $Computers)
 {
    $Svr = $Computer.servername
    $Environment = $Computer.Environment

    write-host "Processing server : $svr" -ForegroundColor Green
    IF (Test-Connection -ComputerName $SVR -Count 1 -ErrorAction SilentlyContinue) 
    {
        try
        {
            $dsk = Get-WmiObject win32_volume -ComputerName $svr -ErrorAction SilentlyContinue

            foreach ($entry in $dsk)
            {

                $ServerName = ($entry.pscomputername)
                $Volume     = ($entry.name)
                $capacity = ($entry.capacity/1gb)
                $FreeSpaceGB = ($entry.freespace/1gb)
                $DriveType   = ($entry.DriveType)

                IF ($Capacity -gt 0)
                {
                    $FreeSpavepct = ($FreeSpaceGB/$capacity)*100
                }
                else 
                {
                    $FreeSpavepct = 0
                }


                if (($volume -notlike "*Volume{*") -and ($DriveType -eq 3))
                {
                    $SQL = 
                    "INSERT GMODBA.alt.DiskSpaceReportv2
                    select '$Collected','$svr','$Environment','$volume','$capacity','$FreeSpaceGB','$FreeSpavepct'
                    "

                    run-sql -dataSource EMDWPROD -database master -sqlCommand $sql
                }
 
            }
        }
        catch
        {
            write-host "Error processing server $svr"

            $SQL = 
            "INSERT GMODBA.alt.DiskSpaceReportv2
            select '$Collected','$svr','Error','0','0','0','0'
            "

            run-sql -dataSource EMDWPROD -database master -sqlCommand $sql

        }

    }
    ELSE
    {
        WRITE-HOST "Server : $svr not available" -ForegroundColor Red

    }

 }
$End = Get-Date


