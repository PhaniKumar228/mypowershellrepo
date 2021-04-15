#Create your SQL connection string, and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "exec rpt_GetServerList_DeadLockViewerPS "
        $query = $query + "@env = 'PRD' "
        #$query = $query + "@ServerName = 'ha34' "
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
    # write-host "DBServer: " $a.ServerName   ; 
 
    $tableName = "HeapInformation"
    $sql = ""
    
    $sql = $sql + "create table #report (dbname varchar(250), tablename varchar(500), numRows bigint null, collectiondate date) "
    $sql = $sql + "declare @db_NAME varchar(250) "
    $sql = $sql + "declare @sql varchar(max) "
   
   $sql = $sql + "Select @SQL = ' SELECT ''?'' as DbName, "
         $sql = $sql + "SCH.name + ''.'' + TBL.name AS TableName , "
		 $sql = $sql + "pa.rows, getdate() "
$sql = $sql + "FROM [?].sys.tables AS TBL "
$sql = $sql + "outer apply (select max(rows) rows from [?].sys.partitions pa where pa.OBJECT_ID = tbl.OBJECT_ID) pa "
     $sql = $sql + "INNER JOIN [?].sys.schemas AS SCH "
         $sql = $sql + "ON TBL.schema_id = SCH.schema_id "
     $sql = $sql + "INNER JOIN [?].sys.indexes AS IDX "
         $sql = $sql + "ON TBL.object_id = IDX.object_id "
            $sql = $sql + "AND IDX.type = 0  "
	$sql = $sql + "WHERE isnull(pa.rows,1) >99 "
$sql = $sql + "ORDER BY TableName	'"
	$sql = $sql + "INSERT INTO #report "
	$sql = $sql + "EXEC sp_msforeachdb @SQL "

	$sql = $sql + " delete FROM #report where dbname in ('master','msdb','tempdb','gmodba') " 
$sql = $sql + "SELECT '" + $a.Servername+ "' AS ServerName, * FROM #report order by numrows desc "

$sql = $sql + "drop table #report "

    write-host $sql
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