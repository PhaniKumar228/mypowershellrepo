param([String]$Environment="",[String]$Instance="",[String]$ServerList="") 

#write-host $args[0]


#Create your SQL connection string, and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
 write-host "test";
#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "exec rpt_GetServerList_withEnv "
        #$query = $query + " @servername = 'BOSSQLPRD86A' "
        if ($Environment -ne "" )
        {
         $query = $query + " @env = '" + $Environment + "' "
        
        }

        if ($Instance -ne "" )
        {
         $query = $query + " ,@servername = '" + $Instance + "' "
        
        }
        
        if ($ServerList -ne "" )
        {
         $query = $query + " ,@ServerList = '" + $ServerList+ "' "
        
        }
       
 
# write-host $query;
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


    # capture restart times and authschemes per instance to eMDW..DB_ServerValidationRestartInfo

    $sql =  "        select @@ServerName as ServerName ,'" + $CollectionTime + "' as collectiontime, login_time as RestartTime , auth_scheme, convert(nvarchar(50),SERVERPROPERTY('productversion')) SQLVersion,  convert(nvarchar(50),SERVERPROPERTY ('productlevel')) SPLevel, convert(nvarchar(50),SERVERPROPERTY ('edition')) SQLEdition "
    $sql =  $sql + " from master.dbo.sysprocesses a "
    $sql =  $sql + " cross join sys.dm_exec_connections b "
    $sql =  $sql + " where a.spid = 1 "
    $sql =  $sql + " and b.session_id=@@spid "

 
    $tableName = "DB_ServerValidationRestartInfo"
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
 
    $reader.Close()
    # capture db configs per instance to eMDW..DB_ServerValidationDatabases
    $sql =  "       select @@ServerName as ServerName, '" + $CollectionTime + "' as collectiontime, SERVERPROPERTY('ComputerNamePhysicalNetBIOS') NodeName  ,* from sys.databases "
   
 
    $tableName = "DB_ServerValidationDatabases"
    $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
    #$commandSourceData .CommandTimeout = '300'

    $reader = $commandSourceData.ExecuteReader()
    
   
        $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $DestinationConnectionString
        $bulkCopy.DestinationTableName = $tableName
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 0
        $bulkCopy.WriteToServer($reader)
 


     $reader.Close()
	 
    # capture instance config to eMDW..DB_ServerValidationConfigurations
    $sql =  "      select @@ServerName as ServerName ,'" + $CollectionTime + "' as collectiontime, configuration_id,name,value from sys.configurations "
   
 
    $tableName = "DB_ServerValidationConfigurations"
    $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
    #$commandSourceData .CommandTimeout = '300'

    $reader = $commandSourceData.ExecuteReader()
    
   
        $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $DestinationConnectionString
        $bulkCopy.DestinationTableName = $tableName
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 0
        $bulkCopy.WriteToServer($reader)
 



   $pos = $a.Servername.IndexOf("\")
    
    if ($pos -gt 0)
    {
            $RemoteServer = $a.Servername.Substring(0, $pos)
    }
    else
    {
            $RemoteServer = $a.Servername
    }
    $ServicesList = ""

        $options = New-CimSessionOption -Protocol Dcom
        $cim = New-CimSession -SessionOption $options -computername $RemoteServer 
        
        #-ComputerName $RemoteServer
        $ServicesList = Get-CimInstance -CimSession $cim -ClassName win32_service   -OperationTimeoutSec 30 | select Name, State, Startname, StartMode, DisplayName

        ForEach($Service in $ServicesList)
        {
          $ServiceName = $Service.Name;
          $ServiceState= $Service.State;
          $ServiceLogin= $Service.Startname;
          $ServiceStartMode =  $Service.StartMode
          $ServiceDisplayName = $Service.Displayname;


          #write-host $CollectionTime,$a.Servername,$Service.Name, $Service.State, $Service.Startname, $Service.StartMode
          $ServiceStateInsert = "Insert DB_ServerServicesValidation values ('"+ $a.Servername + "', '$CollectionTime','" + $ServiceName +"', '" + $ServiceState+ "', '" + $ServiceLogin + "', '" + $ServiceStartMode+ "', '" + $ServiceDisplayName+ "')"
          
          #write-host $ServiceStateInsert 
          if($ServiceName -ne "")
          {
            Invoke-Sqlcmd -ServerInstance EMDWProd -Database EMDW -Query $ServiceStateInsert 
          }

        }



     $reader.Close()

		 
    # capture instance config to eMDW..DB_ServerValidationConfigurations
$sql =  "      if (SELECT ISNULL(SERVERPROPERTY ('IsHadrEnabled'),0)) = 1 "
$sql =  $SQL + " BEGIN "
$sql =  $SQL + "      select '" + $CollectionTime + "' as collectiontime,replica_server_name,role_desc,availability_mode_desc,failover_mode_desc, db1.name "
$sql =  $SQL + "      from sys.dm_hadr_availability_replica_states a "
$sql =  $SQL + " inner join sys.availability_groups g "
$sql =  $SQL + " on a.group_id = g.group_id "
$sql =  $SQL + " inner join sys.availability_replicas r "
$sql =  $SQL + " on a.replica_id = r.replica_id "
$sql =  $SQL + "  inner join sys.availability_databases_cluster db "
$sql =  $SQL + " on db.group_id = g.group_id "
$sql =  $SQL + " inner join sys.databases db1 "
$sql =  $SQL + " on db1.group_database_id = db.group_database_id "
$sql =  $SQL + " and replica_server_name = @@Servername"
$sql =  $SQL + " order by 1,2 "
$sql =  $SQL + " END"
# write-host $sql
     $tableName = "DB_ServerDatabases_AAG"
    $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
    #$commandSourceData .CommandTimeout = '300'

    $reader = $commandSourceData.ExecuteReader()
    
    #$commandSourceData .CommandTimeout = '300'
    
   
        $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $DestinationConnectionString
        $bulkCopy.DestinationTableName = $tableName
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 0
        $bulkCopy.WriteToServer($reader)
 


     $reader.Close()




<#


#$sql =  "        select login_time as RestartTime from master.dbo.sysprocesses where spid = 1 " 
#$dataAdapter2 = new-object "System.Data.SqlClient.SqlDataAdapter" ($sql, $SourceConnectionString )
#$dataAdapter2.Fill($dataSet2) | Out-Null

     foreach($b in $dataSet2.Tables[0].Rows)
        {
            write-host "dbname: " $b.RestartTime; 
        }

   

select name,state_desc from sys.databases where state_desc <> 'online'
select login_time as RestartTime from master.dbo.sysprocesses where spid = 1
select auth_scheme from sys.dm_exec_connections where session_id=@@spid
select configuration_id,name,value from sys.configurations
select @@servername ClusterName, SERVERPROPERTY('ComputerNamePhysicalNetBIOS') NodeName where SERVERPROPERTY('IsClustered') = 1
select name dbname,@@servername ClusterName, 
      SERVERPROPERTY('ComputerNamePhysicalNetBIOS') NodeName  from sys.databases where state_desc = 'ONLINE' and database_id > 4


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
  #>  
  
  
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

    #call the cluster info collection with the same collectiontime.
    & D:\gmopowershell\FetchClusterState.ps1 $CollectionTime
#Close the connection as soon as you are done with it

$ServerAConnection.Close()