#Define your connection string - i am using integrated security in the example, but you can tweak it to work with 
# sql authentication.  check out http://connectionstrings.com/ for examples
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
 write-host "test";
#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "exec [rpt_GetServerList_withEnv_ExecStats] "
        #$query = $query + "@ServerName = 'BOSSQLPRD12a' , "
        $query = $query + " @env = 'prd' "
       
 
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

    $tableName = "DB_ExecStats_Capture"

    $sql = "DECLARE @DT DATETIME ; "
    $sql = $sql + "SET @DT = GETDATE() ;  "
    $sql = $sql + "Select " + $a.increment_id + " as increment_id, @dt as capture_time,'ByReads' as collectiontype,*  "
    $sql = $sql + "from ("
	$sql = $sql + "SELECT  *, RANK()  over (PARTITION by DatabaseName order by DatabaseName  desc,total_logical_reads DESC ) as Rank "
    $sql = $sql + 	"FROM ( "
    $sql = $sql + 	"SELECT top 10000 "
    $sql = $sql + 	"@@Servername as ServerName, "
    $sql = $sql + 	"isnull(DB_name(qp.dbid),'unknown') as DatabaseName, "
    $sql = $sql + 	"OBJECT_NAME(qp.objectid) as ObjectName, "
    $sql = $sql + 	"SUBSTRING(qt.TEXT, (qs.statement_start_offset/2)+1 , "
    $sql = $sql + 	"((CASE qs.statement_end_offset "
	$sql = $sql + 	"WHEN -1 THEN DATALENGTH(qt.TEXT) "
	$sql = $sql + 	"ELSE qs.statement_end_offset "
	$sql = $sql + 	"END - qs.statement_start_offset)/2)+1)as Statement, "
	$sql = $sql + 	"qs.execution_count, "
	$sql = $sql + 	"qs.total_logical_reads, qs.last_logical_reads, "
	$sql = $sql + 	"qs.total_logical_writes, qs.last_logical_writes, "
	$sql = $sql + 	"qs.total_worker_time, "
	$sql = $sql + 	"qs.last_worker_time, "
	$sql = $sql + 	"qs.total_elapsed_time/1000000 total_elapsed_time_in_S, "
	$sql = $sql + 	"qs.last_elapsed_time/1000000 last_elapsed_time_in_S, "
	$sql = $sql + 	"qs.last_execution_time, qs.sql_handle, qs.plan_handle,"
	#$sql = $sql + 	"qp.query_plan "
	$sql = $sql + 	"NULL"
	$sql = $sql + 	"FROM sys.dm_exec_query_stats qs "
	$sql = $sql + 	"CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt "
	$sql = $sql + 	"CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp "
	$sql = $sql + 	"where total_logical_reads > 1000 and qp.dbid > 4 "
	$sql = $sql + 	"ORDER BY qs.total_logical_reads DESC "
	#$sql = $sql + 	"-- ORDER BY qs.total_logical_writes DESC -- logical writes "
	#$sql = $sql + 	"-- ORDER BY qs.total_worker_time DESC -- CPU time "
	$sql = $sql + 	") as A "
$sql = $sql + 	") as b "
$sql = $sql + 	"where b.Rank <30 "


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






 $tableName = "DB_ExecStats_Capture"

    $sql = "DECLARE @DT DATETIME ; "
    $sql = $sql + "SET @DT = GETDATE() ;  "
    $sql = $sql + "Select " + $a.increment_id + " as increment_id, @dt as capture_time,'ByCPUTime' as collectiontype,*  "
    $sql = $sql + "from ("
	$sql = $sql + "SELECT  *, RANK()  over (PARTITION by DatabaseName order by DatabaseName  desc,total_worker_time DESC ) as Rank "
    $sql = $sql + 	"FROM ( "
    $sql = $sql + 	"SELECT top 10000 "
    $sql = $sql + 	"@@Servername as ServerName, "
    $sql = $sql + 	"isnull(DB_name(qp.dbid),'unknown') as DatabaseName, "
    $sql = $sql + 	"OBJECT_NAME(qp.objectid) as ObjectName, "
    $sql = $sql + 	"SUBSTRING(qt.TEXT, (qs.statement_start_offset/2)+1 , "
    $sql = $sql + 	"((CASE qs.statement_end_offset "
	$sql = $sql + 	"WHEN -1 THEN DATALENGTH(qt.TEXT) "
	$sql = $sql + 	"ELSE qs.statement_end_offset "
	$sql = $sql + 	"END - qs.statement_start_offset)/2)+1)as Statement, "
	$sql = $sql + 	"qs.execution_count, "
	$sql = $sql + 	"qs.total_logical_reads, qs.last_logical_reads, "
	$sql = $sql + 	"qs.total_logical_writes, qs.last_logical_writes, "
	$sql = $sql + 	"qs.total_worker_time, "
	$sql = $sql + 	"qs.last_worker_time, "
	$sql = $sql + 	"qs.total_elapsed_time/1000000 total_elapsed_time_in_S, "
	$sql = $sql + 	"qs.last_elapsed_time/1000000 last_elapsed_time_in_S, "
	$sql = $sql + 	"qs.last_execution_time, qs.sql_handle, qs.plan_handle,"
	#$sql = $sql + 	"qp.query_plan "
	$sql = $sql + 	" NULL "
	$sql = $sql + 	"FROM sys.dm_exec_query_stats qs "
	$sql = $sql + 	"CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt "
	$sql = $sql + 	"CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp "
	$sql = $sql + 	"where qs.total_elapsed_time/1000000 > 0   and qp.dbid > 4"
	$sql = $sql + 	"ORDER BY qs.total_worker_time DESC "
	#$sql = $sql + 	"-- ORDER BY qs.total_logical_writes DESC -- logical writes "
	#$sql = $sql + 	"-- ORDER BY qs.total_worker_time DESC -- CPU time "
	$sql = $sql + 	") as A "
$sql = $sql + 	") as b "
$sql = $sql + 	"where b.Rank <30 "


     $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
    $commandSourceData.CommandTimeout = 300

    $reader = $commandSourceData.ExecuteReader()
    
   
        $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $DestinationConnectionString
        $bulkCopy.DestinationTableName = $tableName
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 0
        $bulkCopy.WriteToServer($reader)

       $reader.Close()


       




 $tableName = "DB_ExecStats_Capture"

    $sql = "DECLARE @DT DATETIME ; "
    $sql = $sql + "SET @DT = GETDATE() ;  "
    $sql = $sql + "Select " + $a.increment_id + " as increment_id, @dt as capture_time,'ByWrites' as collectiontype,*  "
    $sql = $sql + "from ("
	$sql = $sql + "SELECT  *, RANK()  over (PARTITION by DatabaseName order by DatabaseName  desc,total_logical_writes DESC ) as Rank "
    $sql = $sql + 	"FROM ( "
    $sql = $sql + 	"SELECT top 10000 "
    $sql = $sql + 	"@@Servername as ServerName, "
    $sql = $sql + 	"isnull(DB_name(qp.dbid),'unknown') as DatabaseName, "
    $sql = $sql + 	"OBJECT_NAME(qp.objectid) as ObjectName, "
    $sql = $sql + 	"SUBSTRING(qt.TEXT, (qs.statement_start_offset/2)+1 , "
    $sql = $sql + 	"((CASE qs.statement_end_offset "
	$sql = $sql + 	"WHEN -1 THEN DATALENGTH(qt.TEXT) "
	$sql = $sql + 	"ELSE qs.statement_end_offset "
	$sql = $sql + 	"END - qs.statement_start_offset)/2)+1)as Statement, "
	$sql = $sql + 	"qs.execution_count, "
	$sql = $sql + 	"qs.total_logical_reads, qs.last_logical_reads, "
	$sql = $sql + 	"qs.total_logical_writes, qs.last_logical_writes, "
	$sql = $sql + 	"qs.total_worker_time, "
	$sql = $sql + 	"qs.last_worker_time, "
	$sql = $sql + 	"qs.total_elapsed_time/1000000 total_elapsed_time_in_S, "
	$sql = $sql + 	"qs.last_elapsed_time/1000000 last_elapsed_time_in_S, "
	$sql = $sql + 	"qs.last_execution_time, qs.sql_handle, qs.plan_handle,"
	#$sql = $sql + 	"qp.query_plan "
	$sql = $sql + 	" NULL"
	$sql = $sql + 	"FROM sys.dm_exec_query_stats qs "
	$sql = $sql + 	"CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt "
	$sql = $sql + 	"CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp "
	$sql = $sql + 	"where qs.total_logical_writes > 0  and qp.dbid > 4"
	$sql = $sql + 	"ORDER BY qs.total_worker_time DESC "
	#$sql = $sql + 	"-- ORDER BY qs.total_logical_writes DESC -- logical writes "
	#$sql = $sql + 	"-- ORDER BY qs.total_worker_time DESC -- CPU time "
	$sql = $sql + 	") as A "
$sql = $sql + 	") as b "
$sql = $sql + 	"where b.Rank <30 "


     $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
    $commandSourceData.CommandTimeout = 300

    $reader = $commandSourceData.ExecuteReader()
    
   
        $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $DestinationConnectionString
        $bulkCopy.DestinationTableName = $tableName
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 0
        $bulkCopy.WriteToServer($reader)

       $reader.Close()
  
  



 $tableName = "DB_ExecStats_Capture"

    $sql = "DECLARE @DT DATETIME ; "
    $sql = $sql + "SET @DT = GETDATE() ;  "
    $sql = $sql + "Select " + $a.increment_id + " as increment_id, @dt as capture_time,'ByDuration' as collectiontype,*  "
    $sql = $sql + "from ("
	$sql = $sql + "SELECT  *, RANK()  over (PARTITION by DatabaseName order by DatabaseName  desc,total_logical_writes DESC ) as Rank "
    $sql = $sql + 	"FROM ( "
    $sql = $sql + 	"SELECT top 10000 "
    $sql = $sql + 	"@@Servername as ServerName, "
    $sql = $sql + 	"isnull(DB_name(qp.dbid),'unknown') as DatabaseName, "
    $sql = $sql + 	"OBJECT_NAME(qp.objectid) as ObjectName, "
    $sql = $sql + 	"SUBSTRING(qt.TEXT, (qs.statement_start_offset/2)+1 , "
    $sql = $sql + 	"((CASE qs.statement_end_offset "
	$sql = $sql + 	"WHEN -1 THEN DATALENGTH(qt.TEXT) "
	$sql = $sql + 	"ELSE qs.statement_end_offset "
	$sql = $sql + 	"END - qs.statement_start_offset)/2)+1)as Statement, "
	$sql = $sql + 	"qs.execution_count, "
	$sql = $sql + 	"qs.total_logical_reads, qs.last_logical_reads, "
	$sql = $sql + 	"qs.total_logical_writes, qs.last_logical_writes, "
	$sql = $sql + 	"qs.total_worker_time, "
	$sql = $sql + 	"qs.last_worker_time, "
	$sql = $sql + 	"qs.total_elapsed_time/1000000 total_elapsed_time_in_S, "
	$sql = $sql + 	"qs.last_elapsed_time/1000000 last_elapsed_time_in_S, "
	$sql = $sql + 	"qs.last_execution_time, qs.sql_handle, qs.plan_handle,"
	#$sql = $sql + 	"qp.query_plan "
	$sql = $sql + 	" NULL "
	$sql = $sql + 	"FROM sys.dm_exec_query_stats qs "
	$sql = $sql + 	"CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt "
	$sql = $sql + 	"CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp "
	$sql = $sql + 	"where qs.total_elapsed_time/1000000 > 0  and qp.dbid > 4"
	$sql = $sql + 	"ORDER BY qs.total_elapsed_time DESC "
	#$sql = $sql + 	"-- ORDER BY qs.total_logical_writes DESC -- logical writes "
	#$sql = $sql + 	"-- ORDER BY qs.total_worker_time DESC -- CPU time "
	$sql = $sql + 	") as A "
$sql = $sql + 	") as b "
$sql = $sql + 	"where b.Rank < 30 "


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


#Close the connection as soon as you are done with it

$ServerAConnection.Close()