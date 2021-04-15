#Define your connection string - i am using integrated security in the example, but you can tweak it to work with 
# sql authentication.  check out http://connectionstrings.com/ for examples
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
 write-host "test";
#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "exec [rpt_GetServerList_withEnv_MemCPU_ByDB] "
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

    $tableName = "DB_MemUsage_ByDatabase_ByDay"

    $sql = "DECLARE @DT DATETIME ; "
    $sql = $sql + "Declare @TheDate datetime; "
    $sql = $sql + "Select @TheDate = getdate(); "
    $sql = $sql + "With DB_MemUsed as "
    $sql = $sql + "( "
    $sql = $sql + "SELECT  "
    $sql = $sql + "@@ServerName as Servername, db_name(Database_ID) as databasename,Database_ID, "
		    $sql = $sql + "COUNT (*) * 8 / 1024 AS MBUsed "
    $sql = $sql + "FROM	sys.dm_os_buffer_descriptors "
    $sql = $sql + "where Database_ID > 4 "
    $sql = $sql + "and Database_ID  <> 32767 "
    $sql = $sql + "GROUP BY	Database_ID "
    $sql = $sql + ") "
    $sql = $sql + "SELECT "
    $sql = $sql + "@TheDate," + $a.increment_id + ",Servername, databasename, MBUsed,  "
    $sql = $sql + "CAST(MBUsed * 1.0 / SUM(MBUsed) OVER() * 100.0 AS DECIMAL(5, 2)) AS MBUsedPercent "
    $sql = $sql + "FROM DB_MemUsed "
    $sql = $sql + "WHERE Database_ID > 4  "
    $sql = $sql + "AND Database_ID <> 32767  "
    $sql = $sql + "ORDER BY 4 desc OPTION (RECOMPILE); "

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
  

	    $tableName = "DB_CPUUsage_ByDatabase_ByDay"
	    $sql = "DECLARE @DT DATETIME ; "
	    $sql = $sql + "Declare @TheDate datetime; "
	    $sql = $sql + "Select @TheDate = getdate(); "
	    $sql = $sql + "WITH DB_CPU_Stats "
		$sql = $sql + "AS "
		$sql = $sql + "(SELECT DatabaseID, DB_Name(DatabaseID) AS DatabaseName, SUM(total_worker_time) AS [CPU_Time_Ms] "
		$sql = $sql + "FROM sys.dm_exec_query_stats AS qs "
		$sql = $sql + "CROSS APPLY (SELECT CONVERT(int, value) AS [DatabaseID]  "
		$sql = $sql + "FROM sys.dm_exec_plan_attributes(qs.plan_handle) "
		$sql = $sql + "WHERE attribute = N'dbid') AS F_DB "
		$sql = $sql + "GROUP BY DatabaseID) "
		$sql = $sql + "SELECT @TheDate," +$a.increment_id + ", @@ServerName,[name] as DatabaseName, isnull([CPU_Time_Ms],0)[CPU_Time_Ms],  "
		$sql = $sql + "CAST([CPU_Time_Ms] * 1.0 / SUM([CPU_Time_Ms]) OVER() * 100.0 AS DECIMAL(5, 2)) AS [CPUPercent] "
		$sql = $sql + "FROM sys.databases db "
		$sql = $sql + "left outer join DB_CPU_Stats a "
		$sql = $sql + "on db.database_id = a.DatabaseID "
		$sql = $sql + "and db.name = a.DatabaseName "
		$sql = $sql + "WHERE Database_ID > 4 "
		$sql = $sql + "AND Database_ID <> 32767 "
		$sql = $sql + " OPTION (RECOMPILE); "

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