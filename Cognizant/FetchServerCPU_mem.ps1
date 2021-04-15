
#get-cluster BOSSQLCLUAT07

#get-clusternode -Cluster BOSSQLUAT12A

#get-clusterownernode -Cluster BOSSQLUAT12A -Group "SQL Server Group"


#Create your SQL connection string, and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
$ServerAConnection.Open()


$dataSet = new-object "System.Data.DataSet" "DBServers"

$dataSetB = new-object "System.Data.DataSet" "LocalInfo"

$query = "SET NOCOUNT ON;"
         $query = $query + " exec rpt_GetServerList_DBAPortal "
      #  $query = $query + "@ServerName = 'BOSSQLprd77' "
        #$query = $query + "and d.databasename = 'SHSDev' "
       
 
# write-host $query;
#Create a DataAdapter which youll use to populate the DataSet with the results
$dataAdapter = new-object "System.Data.SqlClient.SqlDataAdapter" ($query, $ServerAConnection)
$dataAdapter.Fill($dataSet) #| Out-Null



#Get Groups and current owners
#get-clustergroup -Cluster BOSSQLUAT12A 

    #$CollectionTime = date;
    #write-host $args[0]
foreach($a in $dataSet.Tables[0].Rows)
{

    $clustername = $a.ServerName
    
    $ServerBConnectionString = "Data Source=" +  $a.ServerName + ";Initial Catalog=master;Integrated Security=SSPI;"
    $ServerBConnection = new-object system.data.SqlClient.SqlConnection($ServerBConnectionString);
    $ServerBConnection.Open()
    
$query2 = "
		If @@Version like '%2005%' OR @@VERSION like '%2008%'
		BEGIN
		
          exec('Select cpu_count, ((physical_memory_in_bytes/1024)/1024)as physical_memory_MB, hyperthread_ratio From sys.dm_os_sys_info')
		END
        ELSE
		BEGIN
		  exec('Select cpu_count, (physical_memory_kb/1024) as physical_memory_MB, hyperthread_ratio From sys.dm_os_sys_info' )
		END "; 
 
    # write-host $query;
    #Create a DataAdapter which youll use to populate the DataSet with the results
    $dataAdapterB = new-object "System.Data.SqlClient.SqlDataAdapter" ($query2, $ServerBConnection)
    
       $dataSetB.Clear()
    $dataAdapterB.Fill($dataSetB) #| Out-Null
    $B = $dataSetB.Tables[0].Rows[0]

    write-host $a.ServerName $B.cpu_count
    
    #get-clustergroup -Cluster $clustername | Select name
    #get-clusterresource -Cluster $clustername | Select name,state,group,type

    #get groups and states/nodes
    $cmd = $ServerAConnection.CreateCommand()   

    #$output | foreach {

        $sql = " UPDATE DB_ServerData "
        $sql = $sql +  " SET Physical_memory_MB = " + $B.physical_memory_MB + ","
        $sql = $sql +  "  CPU_Count = " + $B.cpu_count + ","
        $sql = $sql +  "  Hyperthread_Ratio = " + $B.hyperthread_ratio 
        $sql = $sql +  " Where SQLServername = '" + $a.ServerName + "'"


    
        $cmd.CommandText = $sql
        #Execute Query
        $cmd.ExecuteNonQuery()
        write-host $sql

      #  }
       $dataSetB.Clear()
       $ServerBConnection.Close()

    }