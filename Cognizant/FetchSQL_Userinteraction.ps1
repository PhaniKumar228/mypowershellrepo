#Define your connection string - i am using integrated security in the example, but you can tweak it to work with 
# sql authentication.  check out http://connectionstrings.com/ for examples
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
 write-host "test";
#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "exec [rpt_GetServerList_withEnv_DBUtilization_IndexStats] "
        #$query = $query + "@ServerName = 'BOSSQLdev08a'  "
        #$query = $query + " @env = 'prd' "
       
 
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

    $tableName = "DB_Utilization_IndexStats"

    $sql = "DECLARE @DT DATETIME ; "
    $sql = $sql + "Declare @TheDate datetime; "
    $sql = $sql + "Select @TheDate = getdate(); "
    
    $sql = $sql + "SELECT "
           #$sql = $sql + "--ServerRestartedDate = (SELECT CREATE_DATE FROM sys.databases where name='tempdb'), "
           $sql = $sql + "@TheDate," + $a.increment_id + ", "
           $sql = $sql + "@@ServerName as ServerName, "
	       $sql = $sql + "DB_NAME(a.database_id) AS DatabaseName, "
           #$sql = $sql + "--OBJECT_NAME(object_id, database_id) as ObjectName, "
           #$sql = $sql + "--OBJECT_NAME(index_id, database_id) as IXName, "
           $sql = $sql + "max(last_user_seek) last_user_seek, "
           $sql = $sql + "max(last_user_scan) last_user_scan, "
           $sql = $sql + "max(last_user_lookup) last_user_lookup, "
           $sql = $sql + "max(last_user_update) last_user_update, "
           $sql = $sql + "max(last_system_seek) last_system_seek, "
           $sql = $sql + "max(last_system_scan) last_system_scan, "
           $sql = $sql + "max(last_system_lookup) last_system_lookup, "
           $sql = $sql + "       max(last_system_update) last_system_update, "
           $sql = $sql + "max(user_seeks) user_seeks, "
           $sql = $sql + "max(user_scans) user_scans, "
           $sql = $sql + "max(user_lookups) user_lookups, "
           $sql = $sql + "max(user_updates) user_updates, "
           $sql = $sql + "max(system_seeks) system_seeks, "
           $sql = $sql + "max(system_scans) system_scans, "
    $sql = $sql + "       max(system_lookups) system_lookups "
    $sql = $sql + "FROM  sys.databases a "
    $sql = $sql + "left outer join sys.dm_db_index_usage_stats b "
    $sql = $sql + "on a.name = DB_NAME(b.database_id) "

    $sql = $sql + "WHERE  DB_NAME(a.database_id) not in ('master','model','msdb','tempdb','gmodba') "
    $sql = $sql + "group by db_name(a.database_id) "
    $sql = $sql + "order by db_name(a.database_id) "
    #WRITE-HOST $SQL
    $sourceConnection.ConnectionString = $SourceConnectionString
    $sourceConnection.open()
    $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
    $commandSourceData.CommandTimeout = 300

    $reader = $commandSourceData.ExecuteReader()
    
   
        $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $DestinationConnectionString
        $bulkCopy.DestinationTableName = $tableName
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 0
        $bulkCopy.WriteToServer($reader)

       $reader.Close()  

       
	    $tableName = "DB_Utilization_ConnectionStats"
	    $sql = "DECLARE @DT DATETIME ; "
	    $sql = $sql + "Declare @TheDate datetime; "
	    $sql = $sql + "Select @TheDate = getdate(); "
	    $sql = $sql + "	   SELECT "
       	$sql = $sql + "@TheDate," + $a.increment_id + ", "
        $sql = $sql + "@@ServerName , "
       	$sql = $sql + "NAME , "
       	$sql = $sql + "COUNT(STATUS) , "
	   	$sql = $sql + "MAX(Login_Time) , "
	   	$sql = $sql + "MAX(last_batch)  "
	    $sql = $sql + "FROM sys.databases sd "
	    $sql = $sql + "LEFT JOIN "
       	$sql = $sql + "master.dbo.sysprocesses sp ON sd.database_id = sp.dbid "
	    $sql = $sql + "WHERE "
       	$sql = $sql + "database_id > 4 "
        $sql = $sql + "and NAME not in ('master','model','msdb','tempdb','gmodba') "
	    $sql = $sql + "GROUP BY NAME " 
           

	#Write-Host $sql
    #$sourceConnection.ConnectionString = $SourceConnectionString
    #$sourceConnection.open()
    $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
    $commandSourceData.CommandTimeout = 300

    $reader = $commandSourceData.ExecuteReader()
    
   
        $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $DestinationConnectionString
        $bulkCopy.DestinationTableName = $tableName
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 0
        $bulkCopy.WriteToServer($reader)

       $reader.Close()  
	
	
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
#Write-Host "Write-DataTable$($connectionName):$ex.Message"

#Close the connection as soon as you are done with it

$ServerAConnection.Close()