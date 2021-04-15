$ErrorActionPreference = "SilentlyContinue"
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
         $query = $query + " exec rpt_GetServerList_DBAPortal @env='PRD'"
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
    
$query2 = "select name as alertname from msdb..sysalerts where name = 'DBA__DeadlockAlert' "; 
 
    # write-host $query;
    #Create a DataAdapter which youll use to populate the DataSet with the results
    $dataAdapterB = new-object "System.Data.SqlClient.SqlDataAdapter" ($query2, $ServerBConnection)
    
       $dataSetB.Clear()
    $dataAdapterB.Fill($dataSetB) | Out-Null
    $B = $dataSetB.Tables[0].Rows[0]

    IF ($B.alertname -ne "DBA__DeadlockAlert")
    {
        write-host $a.ServerName $B.alertname
    }
    
   
      #  }
       $dataSetB.Clear()
       $ServerBConnection.Close()

    }