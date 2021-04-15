param([String]$Environment="",[String]$Instance="",[String]$ServerList="") 


#Create your SQL connection string, and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
write-host "test";
#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "exec [rpt_GetServerList_RingBufferCPU_withEnv] "

         #$query = $query + " @servername = 'bossqlprd88' "

        if ($Environment -ne "" )
        {
         $query = $query + " @env = '" + $Environment + "' "
        
        }

        if ($Environment -ne "" )
        {
         $query = $query + " ,"
        
        }

        if ($Instance -ne "" )
        {
         $query = $query + " @servername = '" + $Instance + "' "
        
        }

        if ($Environment -ne "" )
        {
            if ($Instance -ne "" )
                {
                    $query = $query + " ,"
                }
        
        }

        if ($ServerList -ne "" )
        {
         $query = $query + " @ServerList = '" + $ServerList+ "' "
        
        }
       
 
 write-host $query;
#Create a DataAdapter which youll use to populate the DataSet with the results
$dataAdapter = new-object "System.Data.SqlClient.SqlDataAdapter" ($query, $ServerAConnection)
$dataAdapter.Fill($dataSet) #| Out-Null
 
 $ServerAConnection.Open()

$CollectionTime = date;
$sourceConnection  = New-Object System.Data.SqlClient.SQLConnection($SourceConnectionString)
   
$dataSet2 = new-object "System.Data.DataSet" "DBValidation" 

 foreach($a in $dataSet.Tables[0].Rows)
{
  write-host "DBServer: " $a.ServerName; 
  
   # open a connection for the server/database
    $SourceConnectionString = "Data Source="+$a.Servername+";Integrated Security=SSPI;"
    $DestinationConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=True"
     try
    {

    $sql =      " 	declare @MaxSeqNo bigint "
    $sql =      $sql + " declare @LastSeqNo bigint "
    $sql =      $sql + " declare @EventTime datetime "

    $sql =      $sql + " Select @LastSeqNo = " +  $a.SeqNo+1    + " "
    $sql =      $sql + " Select @EventTime = '" +  $a.EventTime    + "' "
    $sql =      $sql + " select @MaxSeqNo  = max(record_id)  "
    $sql =      $sql + " from ( "
    $sql =      $sql + "       select "
    $sql =      $sql + "             record.value('(./Record/@id)[1]', 'int') as record_id, "
    $sql =      $sql + "             record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') as SystemIdle, "
    $sql =      $sql + "             record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') as SQLProcessUtilization, "
    $sql =      $sql + "             timestamp "
    $sql =      $sql + "       from ( "
    $sql =      $sql + "             select timestamp, convert(xml, record) as record "
    $sql =      $sql + "             from sys.dm_os_ring_buffers "
    $sql =      $sql + "             where ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' "
    $sql =      $sql + "             and record like '%<SystemHealth>%') as x "
    $sql =      $sql + "       ) as y "
    $sql =      $sql + " If @MaxSeqNo > @LastSeqNo "
    $sql =      $sql + " BEGIN "
    $sql =      $sql + " 	Select @LastSeqNo =0 "
    $sql =      $sql + " END "

    $sql =      $sql + " select @@Servername as ServerName, record_id as SeqNo, "
    $sql =      $sql + " dateadd(ms, -1 * ((Select cpu_ticks / (cpu_ticks/ms_ticks) from sys.dm_os_sys_info) - [timestamp]), GetDate()) as EventTime, "
    $sql =      $sql + "       SQLProcessUtilization, "
    $sql =      $sql + "       SystemIdle, "
    $sql =      $sql + "       100 - SystemIdle - SQLProcessUtilization as OtherProcessUtilization "
    $sql =      $sql + " from ( "
    $sql =      $sql + "       select "
    $sql =      $sql + "             record.value('(./Record/@id)[1]', 'int') as record_id, "
    $sql =      $sql + "             record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') as SystemIdle, "
    $sql =      $sql + "             record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') as SQLProcessUtilization, "
    $sql =      $sql + "             timestamp "
    $sql =      $sql + "       from ( "
    $sql =      $sql + "             select timestamp, convert(xml, record) as record "
    $sql =      $sql + "             from sys.dm_os_ring_buffers "
    $sql =      $sql + "             where ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' "
    $sql =      $sql + "             and record like '%<SystemHealth>%') as x "
    $sql =      $sql + "       ) as y "
    #$sql =      $sql + " where record_id > @LastSeqNo order by record_id desc   "
    $sql =      $sql + " where dateadd(ms, -1 * ((Select cpu_ticks / (cpu_ticks/ms_ticks) from sys.dm_os_sys_info) - [timestamp]), GetDate())  > @EventTime order by record_id desc   "

#$sql =  $sql + " where event_data_XML.value ('(/event/@timestamp)[1]', 'datetime'     )> '" +$a.xEventTimeStamp + "'; "
 
 write-host $sql
 

        $tableName = "DB_CPU_FromRingBuffer_Raw"
        $sourceConnection.ConnectionString = $SourceConnectionString
        $sourceConnection.open()
        $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
   
    
        $reader = $commandSourceData.ExecuteReader()
    
        $commandSourceData.CommandTimeout = '300'
    
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
       # $reader.close()
    }
    $sourceConnection.close()
    $dataSet2.Clear()
}


#Close the connection as soon as you are done with it

$ServerAConnection.Close()