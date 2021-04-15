param([String]$CollectionTime) 

#get-cluster BOSSQLCLUAT07

#get-clusternode -Cluster BOSSQLUAT12A

#get-clusterownernode -Cluster BOSSQLUAT12A -Group "SQL Server Group"
	function Get-ClusterGroup
	{
	    param($cluster)
	   
	    gwmi -class MSCluster_ResourceGroup -namespace "root\mscluster" -computername $cluster -Authentication PacketPrivacy | add-member -pass NoteProperty Cluster $cluster  |
	    add-member -pass ScriptProperty Node `
	    { gwmi -namespace "root\mscluster" -computerName $this.Cluster -Authentication PacketPrivacy -query "ASSOCIATORS OF {MSCluster_ResourceGroup.Name='$($this.Name)'} WHERE AssocClass = MSCluster_NodeToActiveGroup" | Select -ExpandProperty Name } |
	    add-member -pass ScriptProperty PreferredNodes `
	    { @(,(gwmi -namespace "root\mscluster" -computerName $this.Cluster -Authentication PacketPrivacy -query "ASSOCIATORS OF {MSCluster_ResourceGroup.Name='$($this.Name)'} WHERE AssocClass = MSCluster_ResourceGroupToPreferredNode" | Select -ExpandProperty Name)) }
	 
	}

function Get-ClusterResource
{
    param($cluster)
    gwmi -ComputerName $cluster -Authentication PacketPrivacy -Namespace "root\mscluster" -Class MSCluster_Resource | add-member -pass NoteProperty Cluster $cluster |
    add-member -pass ScriptProperty Node `
    { gwmi -namespace "root\mscluster" -computerName $this.Cluster -Authentication PacketPrivacy -query "ASSOCIATORS OF {MSCluster_Resource.Name='$($this.Name)'} WHERE AssocClass = MSCluster_NodeToActiveResource" | Select -ExpandProperty Name } |
    add-member -pass ScriptProperty Group `
    { gwmi -ComputerName $this.Cluster -Authentication PacketPrivacy -Namespace "root\mscluster" -query "ASSOCIATORS OF {MSCluster_Resource.Name='$($this.Name)'} WHERE AssocClass = MSCluster_ResourceGroupToResource" | Select -ExpandProperty Name }
       
}

#Create your SQL connection string, and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
$ServerAConnection.Open()


$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "select  distinct left(activenode,charindex('_',activenode)-1) as servername  
                           from DB_ServerActiveCluster
                            where isclustered =1
                            and collectiontime = (select max(collectiontime) from DB_ServerActiveCluster)
                            order by 1 desc";
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
    #write-host $clustername 
    
    #get-clustergroup -Cluster $clustername | Select name
    #get-clusterresource -Cluster $clustername | Select name,state,group,type

    #get groups and states/nodes
    $output = get-clustergroup -Cluster $clustername  | Select Name,State,node
    $cmd = $ServerAConnection.CreateCommand()   

    $output | foreach {

        $sql = " insert into DB_clustergroups(collectiontime,clustername,groupname,state,node) "
        $sql = $sql +  " VALUES "
        $sql = $sql +  "('$($CollectionTime)',"
        $sql = $sql +  "'$($clustername)',"
        $sql = $sql +  "'$($_.Name)',"
        $sql = $sql +  "'$($_.State)',"
        $sql = $sql +  "'$($_.node)')"

    
        $cmd.CommandText = $sql
        #Execute Query
        $cmd.ExecuteNonQuery()
        #write-host $sql

        }


    #get resources and states 
    #$output = get-clusterresource -Cluster $clustername
    $output = get-clusterresource -Cluster $clustername | Select name,state,group,type

     $cmd = $ServerAConnection.CreateCommand()   

    $output | foreach {

        $sql = " insert into DB_clusterresources(collectiontime,clustername,name,state,ownergroup,resourcetype) "
        $sql = $sql +  " VALUES "
        $sql = $sql +  "('$($CollectionTime)',"
        $sql = $sql +  "'$($clustername)',"
        $sql = $sql +  "'$($_.Name)',"
        $sql = $sql +  "'$($_.State)',"
        $sql = $sql +  "'$($_.Group)',"
        $sql = $sql +  "'$($_.type)')"

    
        $cmd.CommandText = $sql
        #Execute Query
        $cmd.ExecuteNonQuery()
        #write-host $sql

        }
    }