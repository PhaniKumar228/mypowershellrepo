#Create your SQL connection string, and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "exec rpt_GetServerList_withEnv "
        #$query = $query + "@ServerName = 'BOSSQLPRD34' "
       # $query = $query + "@ServerName = 'BOSSQLDEV08A' "
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
  
   # open a connection for the server/database
    $SourceConnectionString = "Data Source="+$a.Servername+";Integrated Security=SSPI;"
    $DestinationConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=True"
     try
    {
     write-host "DBServer: " $a.ServerName   ; 
 
    $tableName = "DB_TempDBStats"
    $sql = "use tempdb; "
    $sql = $sql + " DECLARE @DT DATETIME ; "
    $sql = $sql + " SET @DT = GETDATE() ;  "
    
    $sql = $sql + "SELECT '" + $a.Servername+ "' AS ServerName, @DT as capture_time, " + $a.tempdbincrement_id  + " as increment_id, "
    $sql = $sql + "SUM (user_object_reserved_page_count)*8/1024 as user_obj_kb, "
    $sql = $sql + "SUM (internal_object_reserved_page_count)*8/1024 as internal_obj_kb, "
    $sql = $sql + "SUM (version_store_reserved_page_count)*8/1024  as version_store_kb, "
    $sql = $sql + "SUM (unallocated_extent_page_count)*8/1024 as freespace_kb, "
    $sql = $sql + "SUM (mixed_extent_page_count)*8/1024 as mixedextent_kb "
    $sql = $sql + "FROM sys.dm_db_file_space_usage "
    
    #write-host $sql
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