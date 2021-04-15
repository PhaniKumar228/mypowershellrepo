

Import-Module FailoverClusters;

#Create your SQL connection string to connect to EMDWProd instance and eMDW database.  -- and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"          
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
#Create a Dataset to hold the DataTable from the InfoBlox_Details table (which hosts the list of all prod sql svr names)
$dataSet = new-object "System.Data.DataSet" "DBServers"

$query = "exec rpt_getserverlist_clusterlist"
#DEBUG - JUST DO THE UAT 12 CLUSTER (comment this out to do the whole cluster list from the proc)
#$query = "select 'BOSSQLUAT12B' as Servername"

#Create a DataAdapter which we will use to populate the DataSet (like a recordset)
$dataAdapter = new-object "System.Data.SqlClient.SqlDataAdapter" ($query, $ServerAConnection)                 
$dataAdapter.Fill($dataSet) | Out-Null
 
$ServerAConnection.Open()
$sourceConnection  = New-Object System.Data.SqlClient.SQLConnection($SourceConnectionString)
   
# the $a is one row from the resultset, and you can reference specific fields with $a.fieldname from within the loop
 foreach($a in $dataSet.Tables[0].Rows)
{
   # QY: This is the area of code where “per server” queries can be executed and then saved back to the central database
   
     $netnames = Get-ClusterResource -Cluster $a.ServerName |
      Where-Object {($_.OwnerGroup -like "*AG*" -or $_.OwnerGroup -like "*SQL*" -or $_.OwnerGroup -like "*tfs*" -or $_.OwnerGroup -like "*DYNATRACEPROD*") -and $_.ResourceType -like "*Network Name*"} 
    
    #for all the network name resources on the cluster...    
    foreach ($netname in $netnames){

        #write-host $netname.name
       # write-host $netname
        
       # write-host "current owner" + $netname.OwnerNode
       # write-host "All Available Nodes:"            
        $allnodes = get-clusternode -Cluster $a.ServerName 
        write-host “ClusteredSQL Instance: ” $a.ServerName " Physical Nodes:" $allnodes
        #write-host "Current Possible Owners:"
        
        #$possibleowners = $netname| Get-ClusterOwnerNode
        
        #foreach ($eachpossibleowner in $possibleowners.OwnerNodes)
        #{
        #    write-host $eachpossibleowner
        #}
        
        #set possible owners to all nodes on the cluster (default)
       # $netname | Set-ClusterOwnerNode -Owners (Get-ClusterNode  -Cluster $a.ServerName  | foreach Name )
        
        # set possible owner to only the current hosting node of the resource.
        #$netname | Set-ClusterOwnerNode -Owners ($netname.OwnerNode.tostring())

        break;
    }

    
}


#Close the connection as soon as you are done with it

$ServerAConnection.Close()