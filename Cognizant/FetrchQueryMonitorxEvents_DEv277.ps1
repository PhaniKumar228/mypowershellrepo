param([String]$Environment="",[String]$Instance="",[String]$ServerList="") 


#Create your SQL connection string, and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
 write-host "test";
#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "exec rpt_GetServerListXEevents_withEnv "
		        $query = $query + " @servername = 'BOSSQLDEV77a' "
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
$EVTFileLocation = "C:\DBAMonitoring\traces\"
 foreach($a in $dataSet.Tables[0].Rows)
{
  write-host "DBServer: " $a.ServerName; 
  
   # open a connection for the server/database
    $SourceConnectionString = "Data Source="+$a.Servername+";Integrated Security=SSPI;"
    $DestinationConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=True"
     try
    {

		if ($a.Servername -eq "BOSSQLHA34")
		{
			$EVTFileLocation = "G:\Ops\ExtEvents\";
		}
		else
		{
			$EVTFileLocation = "C:\DBAMonitoring\traces\";
		}
        $FileOffset = $a.xEventFile_Offset;
    
        $xEventFile_Name = $a.xEventFile_Name;
    
        if ($xEventFile_Name -ne 'NULL')
        {
            $xEventFile_Name = "'" + $xEventFile_Name  + "'"
        }

    write-host $xEventFile_Name
    $sql =  "        SELECT CAST(event_data AS XML) AS event_data_XML, file_name, file_offset"
    $sql =  $sql + " INTO #Events "
    #$sql =  $sql + " FROM sys.fn_xe_file_target_read_file('" +$EVTFileLocation +"sql2012*.xel', null," +$xEventFile_Name+ ", " +$FileOffset+ ") AS F "
 $sql =  $sql + " FROM sys.fn_xe_file_target_read_file('C:\DBAMonitoring\traces\sql2012*.xel', null,NULL,NULL) AS F "
               
 #   $sql =  $sql + " where convert(datetime,substring(event_data,charindex('timestamp=',event_data)+11,24)) > '" +$a.xEventTimeStamp + "'; "
    $sql =  $sql + " SELECT "
    $sql =  $sql + " @@ServerName as ServerName, file_name,file_offset,"
  $sql =  $sql + " event_data_XML.value ('(/event/action[@name=''query_hash''    ]/value)[1]', 'nvarchar(100)'     ) AS query_hash, "
  $sql =  $sql + " DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()), event_data_XML.value ('(/event/@timestamp)[1]', 'datetime'     ))  AS timestamp, "
  $sql =  $sql + " event_data_XML.value ('(/event/@name)[1]', 'vARCHAR(50)'     ) AS EventName, "
  $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''duration''      ]/value)[1]', 'int'        )/1000 AS duration_ms, "
  $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''object_type''   ]/text)[1]', 'varchar(100)'        ) AS object_type, "
  $sql =  $sql + " DB_Name(event_data_XML.value ('(/event/action  [@name=''database_id''   ]/value)[1]', 'int'        )) AS DatabaseName, "
  #$sql =  $sql + " CASE event_data_XML.value ('(/event/data  [@name=''object_type''   ]/text)[1]', 'varchar(100)'        )  "
  #$sql =  $sql + " when 'PROC' then OBJECT_NAME(event_data_XML.value ('(/event/data  [@name=''object_id''      ]/value)[1]', 'BIGINT'))  "
  #$sql =  $sql + " END as ObjectName, "
  #$sql =  $sql + " OBJECT_NAME(event_data_XML.value ('(/event/data  [@name=''object_id''      ]/value)[1]', 'BIGINT'))  as ObjectName,"  
  $sql =  $sql + "   CASE event_data_XML.value ('(/event/@name)[1]', 'vARCHAR(50)'     ) "
  $sql =  $sql + " 	when 'rpc_completed' then  event_data_XML.value ('(/event/data  [@name=''object_name''     ]/value)[1]', 'NVARCHAR(4000)')"
  $sql =  $sql + " 	ELSE OBJECT_NAME(event_data_XML.value ('(/event/data  [@name=''object_id''      ]/value)[1]', 'BIGINT'),event_data_XML.value ('(/event/action  [@name=''database_id''   ]/value)[1]', 'int'        )) "
   $sql =  $sql + " END as ObjectName, "
  $sql =  $sql + " event_data_XML.value ('(/event/action  [@name=''client_hostname''   ]/value)[1]', 'varchar(100)'        ) as HostMachine, "
  $sql =  $sql + " event_data_XML.value ('(/event/action  [@name=''client_app_name''   ]/value)[1]', 'varchar(100)'        ) as client_app_name, "
  $sql =  $sql + " event_data_XML.value ('(/event/action  [@name=''nt_username''   ]/value)[1]', 'varchar(100)'        ) as nt_username, "
  $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''cpu_time''      ]/value)[1]', 'int'        )/1000 AS cpu_time_ms, "
  $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''physical_reads'']/value)[1]', 'BIGINT'        ) AS physical_reads, "
  $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''logical_reads'' ]/value)[1]', 'BIGINT'        ) AS logical_reads, "
  $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''writes''        ]/value)[1]', 'BIGINT'        ) AS writes, "
  $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''row_count''     ]/value)[1]', 'BIGINT'        ) AS row_count, "
  #$sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''statement''     ]/value)[1]', 'NVARCHAR(MAX)') AS statement "
  $sql =  $sql + "   CASE event_data_XML.value ('(/event/@name)[1]', 'vARCHAR(50)'     ) "
  $sql =  $sql + " 	when 'sql_batch_completed' then  event_data_XML.value ('(/event/data  [@name=''batch_text''     ]/value)[1]', 'NVARCHAR(4000)')  "
  $sql =  $sql + " 	ELSE event_data_XML.value ('(/event/data  [@name=''statement''     ]/value)[1]', 'NVARCHAR(4000)')  "
  $sql =  $sql + "   END AS statement "
$sql =  $sql + " FROM #Events "

#$sql =  $sql + " where event_data_XML.value ('(/event/@timestamp)[1]', 'datetime'     )> '" +$a.xEventTimeStamp + "'; "
 
 write-host $sql
 

    try{
        $tableName = "XEvents_Queries"
        $sourceConnection.ConnectionString = $SourceConnectionString
        $sourceConnection.open()
        $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
   
    
        $commandSourceData.CommandTimeout = 3000
        $reader = $commandSourceData.ExecuteReader()
    
        
    }
    Catch
    {
        $ex1 = $_.Exception.Message
        # there is a chance that the xEvent sessions ended or the files rolled over and our historical filename/offset is invalid
        # This catch resets the sql query to look at ALL xel files on the server to basically load everything since its all
        # newer than our last fetch from this machine
        if ($ex1.Contains("The offset"))
        {
                write-host $_.Exception.Message
                write-host $_.Exception.GetType()

                $sql =  "      DROP TABLE #Events  "
                $sql =  $sql + " SELECT CAST(event_data AS XML) AS event_data_XML, file_name, file_offset "
                $sql =  $sql + " into  #Events  FROM sys.fn_xe_file_target_read_file('C:\DBAMonitoring\traces\sql2012*.xel', null,NULL,NULL) AS F "
                #   $sql =  $sql + " where convert(datetime,substring(event_data,charindex('timestamp=',event_data)+11,24)) > '" +$a.xEventTimeStamp + "'; "
                $sql =  $sql + " SELECT "
                $sql =  $sql + " @@ServerName as ServerName, file_name,file_offset,"
                $sql =  $sql + " event_data_XML.value ('(/event/action[@name=''query_hash''    ]/value)[1]', 'nvarchar(100)'     ) AS query_hash, "
                $sql =  $sql + " event_data_XML.value ('(/event/@timestamp)[1]', 'datetime'     ) AS timestamp, "
                $sql =  $sql + " event_data_XML.value ('(/event/@name)[1]', 'vARCHAR(50)'     ) AS EventName, "
                $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''duration''      ]/value)[1]', 'int'        )/1000 AS duration_ms, "
                $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''object_type''   ]/text)[1]', 'varchar(100)'        ) AS object_type, "
                
                $sql =  $sql + " DB_Name(event_data_XML.value ('(/event/action  [@name=''database_id''   ]/value)[1]', 'int'        )) AS DatabaseName, "
                #$sql =  $sql + " CASE event_data_XML.value ('(/event/data  [@name=''object_type''   ]/text)[1]', 'varchar(100)'        )  "
                #$sql =  $sql + " when 'PROC' then OBJECT_NAME(event_data_XML.value ('(/event/data  [@name=''object_id''      ]/value)[1]', 'BIGINT'))  "
                #$sql =  $sql + " END as ObjectName, "
                #$sql =  $sql + " OBJECT_NAME(event_data_XML.value ('(/event/data  [@name=''object_id''      ]/value)[1]', 'BIGINT'))  as ObjectName,"  
                $sql =  $sql + "   CASE event_data_XML.value ('(/event/@name)[1]', 'vARCHAR(50)'     ) "
                $sql =  $sql + " 	when 'rpc_completed' then  event_data_XML.value ('(/event/data  [@name=''object_name''     ]/value)[1]', 'NVARCHAR(4000)')"
                $sql =  $sql + " 	ELSE OBJECT_NAME(event_data_XML.value ('(/event/data  [@name=''object_id''      ]/value)[1]', 'BIGINT'),event_data_XML.value ('(/event/action  [@name=''database_id''   ]/value)[1]', 'int'        )) "
                $sql =  $sql + " END as ObjectName, "
                $sql =  $sql + " event_data_XML.value ('(/event/action  [@name=''client_hostname''   ]/value)[1]', 'varchar(100)'        ) as HostMachine, "
                $sql =  $sql + " event_data_XML.value ('(/event/action  [@name=''client_app_name''   ]/value)[1]', 'varchar(100)'        ) as client_app_name, "
                $sql =  $sql + " event_data_XML.value ('(/event/action  [@name=''nt_username''   ]/value)[1]', 'varchar(100)'        ) as nt_username, "
                $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''cpu_time''      ]/value)[1]', 'int'        )/1000 AS cpu_time_ms, "
                $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''physical_reads'']/value)[1]', 'BIGINT'        ) AS physical_reads, "
                $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''logical_reads'' ]/value)[1]', 'BIGINT'        ) AS logical_reads, "
                $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''writes''        ]/value)[1]', 'BIGINT'        ) AS writes, "
                $sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''row_count''     ]/value)[1]', 'BIGINT'        ) AS row_count, "
                #$sql =  $sql + " event_data_XML.value ('(/event/data  [@name=''statement''     ]/value)[1]', 'NVARCHAR(MAX)') AS statement "
                $sql =  $sql + "   CASE event_data_XML.value ('(/event/@name)[1]', 'vARCHAR(50)'     ) "
                $sql =  $sql + " 	when 'sql_batch_completed' then  event_data_XML.value ('(/event/data  [@name=''batch_text''     ]/value)[1]', 'NVARCHAR(4000)')  "
                $sql =  $sql + " 	ELSE event_data_XML.value ('(/event/data  [@name=''statement''     ]/value)[1]', 'NVARCHAR(4000)')  "
                $sql =  $sql + "   END AS statement "
                $sql =  $sql + " FROM #Events "
                #$sql =  $sql + " where event_data_XML.value ('(/event/@timestamp)[1]', 'datetime'     )> '" +$a.xEventTimeStamp + "'; "
 

                $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
                $commandSourceData.CommandTimeout = 3000
				$reader = $commandSourceData.ExecuteReader()
              
            }
    }
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