





 

 




#Create your SQL connection string, and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"

# Get Server List & truncate table   
#  $query = "SELECT [ServerName] FROM [eMDW].[dbo].[InfoBlox_Details]  where servername = 'sqldag2012.gmo.tld' "
$query = "SET NOCOUNT ON;"
        $query = $query + "`n" +"Truncate Table eMDW.dbo.DB_ServerPrimaryReplica; "
        $query = $query + "`n" + "SELECT g2.name AS GroupName, g1.name AS SubGroupName, s.name AS ServerName "
        $query = $query + "`n" + "FROM [msdb].[dbo].[sysmanagement_shared_server_groups_internal] g1 "
        $query = $query + "`n" + "JOIN [msdb].[dbo].[sysmanagement_shared_server_groups_internal] g2 on g1.parent_id = g2.server_group_id "
        $query = $query + "`n" + "RIGHT OUTER JOIN msdb.dbo.sysmanagement_shared_registered_servers_internal s ON g1.server_group_id = s.server_group_id "
        $query = $query + "`n" + "left OUTER JOIN eMDW.dbo.DB_ServerMain m ON m.ServerName = s.name "
        $query = $query + "`n" + "Where g1.server_type = 0 AND g1.parent_id <> 1 "
        $query = $query + "`n" + "AND m.ActiveFlag = 'Y' AND g2.name IN ('PRD','DRS', 'CLR')   "
        #$query = $query + "`n" + "AND s.name in ('bossqlprd26a') "   
        $query = $query + "`n" + "Order by g2.name, g1.name  "
     # write-host $query;

      
#Create a DataAdapter which youll use to populate the DataSet with the results
$dataAdapter = new-object "System.Data.SqlClient.SqlDataAdapter" ($query, $ServerAConnection)
$dataAdapter.Fill($dataSet) #| Out-Null
 
$ServerAConnection.Open()
$sourceConnection  = New-Object System.Data.SqlClient.SQLConnection($SourceConnectionString)
   
$dataSet2 = new-object "System.Data.DataSet" "DBProperties" 

# the $a is one ROW from above DataSet, and you can reference specific fields with $a.fieldname from within the loop
foreach($a in $dataSet.Tables[0].Rows)
{
     <# write-host “DBServer: ” $a.ServerName ;
      $s = $a.ServerName    
     Get-ServiceAcct $s
    
    $dataTable = Get-ServiceAcct $s | out-DataTable
    
    $DestinationConnectionString = "Data Source="+$ServerA+";Initial Catalog=eMDW;Integrated Security=True"
    $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $DestinationConnectionString  #$connectionString
    $bulkCopy.DestinationTableName = "dbo.DBServrServiceAccount"
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 0
    $bulkCopy.WriteToServer($dataTable)
      #>

    # open a connection for the server/database
    $SourceConnectionString = "Data Source="+$a.Servername+";Integrated Security=SSPI;"
    $DestinationConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=True"
     try
    {
     write-host "DBServer: " $a.ServerName   ; 
 
    $tableName = "DB_ServerPrimaryReplica"
	   
    $sql = "IF SERVERPROPERTY ('IsHadrEnabled') = 1"   
    $sql = $sql + "`n" + "BEGIN"
     $sql = $sql + "`n" + "SELECT  --AGC.name as AGName,"
     $sql = $sql + "`n" + "RCS.replica_server_name AS ServerName,  "
     $sql = $sql + "`n" + "d.name as DBName, d.state_desc as DBState,d.is_read_only, d.is_in_standby,"
     $sql = $sql + "`n" + "ARS.role_desc --, AGL.dns_name as LSName "
     $sql = $sql + "`n" + "FROM  sys.availability_groups_cluster AS AGC"
     $sql = $sql + "`n" + "INNER JOIN sys.dm_hadr_availability_replica_cluster_states AS RCS	ON RCS.group_id = AGC.group_id"
     $sql = $sql + "`n" + "INNER JOIN sys.dm_hadr_availability_replica_states AS ARS	ON ARS.replica_id = RCS.replica_id"
     $sql = $sql + "`n" + "inner join sys.DATABASES d on d.replica_id = ARS.replica_id"
     $sql = $sql + "`n" + "INNER JOIN sys.availability_group_listeners AS AGL ON AGL.group_id = ARS.group_id"
     $sql = $sql + "`n" + "--WHERE  ARS.role_desc = 'PRIMARY'  -- and d.state_desc = 'ONLINE'  -- RECOVERY_PENDING, RESTORING, OFFLINE"
     $sql = $sql + "`n" + " UNION"
     $sql = $sql + "`n" + "SELECT @@servername, name as DBName, state_desc as DBState , is_read_only, is_in_standby, '' AS role_desc FROM sys.DATABASES WHERE replica_id IS NULL  -- dbs not in AAG"
     $sql = $sql + "`n" + "END"
     $sql = $sql + "`n" + "ELSE"
     $sql = $sql + "`n" + "	SELECT @@servername, name as DBName, state_desc as DBState, is_read_only, is_in_standby, '' AS role_desc FROM sys.DATABASES"
     write-host $sql
 
     $sourceConnection.ConnectionString = $SourceConnectionString
     $sourceConnection.open()
    $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)

    $reader = $commandSourceData.ExecuteReader()
       
        $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $DestinationConnectionString
        $bulkCopy.DestinationTableName = $tableName
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 0
        $bulkCopy.WriteToServer($reader)
    }
    catch
    {
        $ex = $_.Exception
    
        Write-Host "Write-DataTable$($connectionName):$ex.Message"
    }
    finally
    {
        $reader.close()
    }
    $sourceConnection.close()
}
 
#Close the connection as soon as you are done with it
$ServerAConnection.Close()