#Create your SQL connection string, and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
 write-host "test";
#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "exec rpt_GetServerList_withEnv "
        #$query = $query + "@ServerName = 'BOSSQLPRD64' "
        #$query = $query + "and d.databasename = 'SHSDev' "
       
 
# write-host $query;
#Create a DataAdapter which youll use to populate the DataSet with the results
$dataAdapter = new-object "System.Data.SqlClient.SqlDataAdapter" ($query, $ServerAConnection)
$dataAdapter.Fill($dataSet) #| Out-Null
 
 $ServerAConnection.Open()


$sourceConnection  = New-Object System.Data.SqlClient.SQLConnection($SourceConnectionString)
   
$dataSet2 = new-object "System.Data.DataSet" "DBProperties" 
 foreach($a in $dataSet.Tables[0].Rows)
{
  write-host "DBServer: " $a.ServerName; 
  
   # open a connection for the server/database
    $SourceConnectionString = "Data Source="+$a.Servername+";Integrated Security=SSPI;"
    $DestinationConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=True"
     try
    {

    $tableName = "DB_WaitStats"
    $sql = "DECLARE @DT DATETIME ; "
    $sql = $sql + "SET @DT = GETDATE() ;  "
    $sql = $sql + "SELECT [wait_type], [waiting_tasks_count], [wait_time_ms], [max_wait_time_ms],   "
    $sql = $sql + " [signal_wait_time_ms], '" + $a.Servername+ "' AS ServerName, @DT as capture_time, " + $a.increment_id + " as increment_id "
    $sql = $sql + "FROM sys.dm_os_wait_stats; "

     $sourceConnection.ConnectionString = $SourceConnectionString
     $sourceConnection.open()
    $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
    #$commandSourceData .CommandTimeout = '300'

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