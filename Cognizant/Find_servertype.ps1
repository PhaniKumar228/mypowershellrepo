function Get-MachineType2
{
    [CmdletBinding()]
   param
   (
        [parameter(mandatory=$false,
                    ValueFromPipeline=$true,
                    ValueFromPipelineByPropertyName=$true,
                    Position=0
        
        )]
        [string[]]$dbservermain
   )

 process
 {

    $run_date = get-date -Format s
    $run_date = $run_date -replace "T"," "

    $tbl_servers = New-Object system.data.datatable "servers_table"
    $col1 = New-Object system.Data.DataColumn collectiondate,([datetime])
    $col2 = New-Object system.Data.DataColumn serverid,([string])
    $col3 = New-Object system.Data.DataColumn instance,([string])
    $col4 = New-Object system.Data.DataColumn servername,([string])
    $col5 = New-Object system.Data.DataColumn type,([string])
    $col6 = New-Object system.Data.DataColumn manufacturer,([string])
    $col7 = New-Object system.Data.DataColumn model,([string])


    $tbl_servers.columns.add($col1)
    $tbl_servers.columns.add($col2)
    $tbl_servers.columns.add($col3)
    $tbl_servers.columns.add($col4)
    $tbl_servers.columns.add($col5)
    $tbl_servers.columns.add($col6)
    $tbl_servers.columns.add($col7)


    foreach ($entry in $dbservermain)
    {
        $server = $_.ServerName
        write-host "Processing server $server" -ForegroundColor Yellow
       
        if ($server -match '\\')
        {
            write-host " - Removing instance name" -ForegroundColor red

            $servername = $server.Substring(0, $server.IndexOf('\'))
        }
        else
        {
            $servername = $server

        }
        
       $ComputerSystemInfo = Get-WmiObject -Class Win32_ComputerSystem -ComputerName $servername -ErrorAction continue

        switch ($ComputerSystemInfo.Model) 
        {
        "VMware Virtual Platform" {$MachineType="VM"}
         default {$MachineType="Physical"}
         }
    
        $newrow = $tbl_servers.NewRow()
        $newrow.collectiondate = $run_date
        $newrow.serverid = $_.dbserverid
        $newrow.instance = $server
        $newrow.servername = $ComputerSystemInfo.PSComputername
        $newrow.type = $machineType
        $newrow.manufacturer = $ComputerSystemInfo.Manufacturer
        $newrow.model = $ComputerSystemInfo.Model

        $tbl_servers.Rows.Add($newrow)

        write-host "$server complete!" -ForegroundColor green
 
 
 }   
     $tbl_servers

}

}


FUNCTION Run-SQL 
{
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

$serverquery = 
"
    SELECT
        dbserverid,
	    servername
    FROM emdw.dbo.db_servermain
    WHERE
	    ActiveFlag = 'Y'
"
$serverstoprocess = run-sql -dataSource 'emdwprod' -database 'master' -sqlCommand $serverquery
cls
$a = $serverstoprocess | select servername, dbserverid | Get-MachineType2

$ConnectionString = "Data Source=emdwprod; Database=emdw; Trusted_Connection=True;";
$bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $ConnectionString
$bulkCopy.DestinationTableName = "server_type"
$bulkCopy.WriteToServer($a)


