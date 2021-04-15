
param([String]$Environment="",[String]$Instance="",[String]$ServerList="") 

#Create your SQL connection string, and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWPROD;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
 write-host "test";
#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "exec  rpt_GetServerList_SessionManager "
        #$query = $query + "@env = 'PRD' "
        #$query = $query + "and d.databasename = 'SHSDev' "
 #$query = $query + " @env = 'DRS' "
       
 if ($Environment -ne "" )
        {
         $query = $query + " @env = '" + $Environment + "' "
        
        }
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

    $tableName = "SessionViewer"

$sql = "  DECLARE @destination_table VARCHAR(4000) ,  "
$sql = $sql + "    @msg NVARCHAR(1000) , 	@SQL nvarchar(max);"

#$sql = $sql + "	IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sessionviewertemp]') AND type in (N'U')) "
#$sql = $sql + "	BEGIN "
$sql = $sql + "	drop TABLE [dbo].[sessionviewertemp];  "
$sql = $sql + "	CREATE TABLE [dbo].[sessionviewertemp]( "
$sql = $sql + "		[dd hh:mm:ss.mss] [varchar](8000) NULL, "
$sql = $sql + "		[session_id] [smallint] NOT NULL, "
$sql = $sql + "		[sql_text] [xml] NULL, "
$sql = $sql + "		[sql_command] [xml] NULL, "
$sql = $sql + "		[login_name] [nvarchar](128) NOT NULL, "
$sql = $sql + "		[wait_info] [nvarchar](4000) NULL, "
$sql = $sql + "		[tran_log_writes] [nvarchar](4000) NULL, "
$sql = $sql + "		[CPU] [varchar](30) NULL, "
$sql = $sql + "		[tempdb_allocations] [varchar](30) NULL, "
$sql = $sql + "		[tempdb_current] [varchar](30) NULL, "
$sql = $sql + "		[blocking_session_id] [smallint] NULL, "
$sql = $sql + "		[blocked_session_count] [varchar](30) NULL, "
$sql = $sql + "		[reads] [varchar](30) NULL, "
$sql = $sql + "		[writes] [varchar](30) NULL, "
$sql = $sql + "		[physical_reads] [varchar](30) NULL, "
$sql = $sql + "		[used_memory] [varchar](30) NULL, "
$sql = $sql + "		[status] [varchar](30) NOT NULL, "
$sql = $sql + "		[tran_start_time] [datetime] NULL, "
$sql = $sql + "		[open_tran_count] [varchar](30) NULL, "
$sql = $sql + "		[percent_complete] [varchar](30) NULL, "
$sql = $sql + "		[host_name] [nvarchar](128) NULL, "
$sql = $sql + "		[database_name] [nvarchar](128) NULL, "
$sql = $sql + "		[program_name] [nvarchar](128) NULL, "
$sql = $sql + "		[start_time] [datetime] NOT NULL, "
$sql = $sql + "		[login_time] [datetime] NULL, "
$sql = $sql + "		[request_id] [int] NULL, "
$sql = $sql + "		[collection_time] [datetime] NOT NULL "
$sql = $sql + "	) ON [PRIMARY]  "
#$sql = $sql + "	END ;"
$sql = $sql + "	SET @destination_table = 'sessionviewertemp' "
$sql = $sql + "	truncate table sessionviewertemp;"
$sql = $sql + "        EXEC dbo.sp_WhoIsActive @get_transaction_info = 1, @get_plans = 0, "
Write-Host $a.SessionCollectionInactiveSpids
if ($a.SessionCollectionInactiveSpids -eq "1")
{
$sql = $sql + "@show_system_spids = 1,@show_sleeping_spids=1, " 
}
$sql = $sql + "            @find_block_leaders = 1,  @get_outer_command=1, @destination_table = @destination_table ; "
$sql = $sql + "		set @SQL  = 'Select ''' + @@ServerName + ''',* ' +' from ' +  @destination_table	"	
$sql = $sql + "	exec sp_executeSQL @SQL "


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