

Import-Module FailoverClusters;

#Create your SQL connection string to connect to EMDWProd instance and eMDW database.  -- and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"          
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
#Create a Dataset to hold the DataTable from the InfoBlox_Details table (which hosts the list of all prod sql svr names)
$dataSet = new-object "System.Data.DataSet" "DBServers"
    #  $query = "SELECT [ServerName] FROM [eMDW].[dbo].[InfoBlox_Details]  where servername = 'sqldag2012.gmo.tld' "


$query = "SELECT top 10 replace(SQLServerInstance,'\INST2','') as ServerName FROM [eMDW].[dbo].[DB_ServerPrdLicensing] ORDER BY 1  "                  
$query = "SELECT top 10 replace(SQLServerInstance,'\INST2','') as ServerName FROM [eMDW].[dbo].[DB_ServerPrdLicensing] where SQLServerinstance like '%9%' ORDER BY 1  "                  


$query = "SELECT distinct top 10  replace(replace(replace(replace(rtrim(ActiveNode),'_1',''),'_2',''),'_3',''),'_4','')  as ServerName 	 FROM [eMDW].[dbo].[DB_ServerPrdLicensing] "
$query = "SELECT distinct top 10  rtrim(ActiveNode)  as ServerName 	 FROM [eMDW].[dbo].[DB_ServerPrdLicensing] "

$query = "exec rpt_getserverlist_clusterlist"
#$query = "select 'BOSSQLUAT12B' as Servername"

#   write-host $query;
#Create a DataAdapter which we will use to populate the DataSet (like a recordset)
$dataAdapter = new-object "System.Data.SqlClient.SqlDataAdapter" ($query, $ServerAConnection)                 
$dataAdapter.Fill($dataSet) | Out-Null
 
$ServerAConnection.Open()


$sourceConnection  = New-Object System.Data.SqlClient.SQLConnection($SourceConnectionString)
   


# the $a is one row from the resultset, and you can reference specific fields with $a.fieldname from within the loop
 foreach($a in $dataSet.Tables[0].Rows)
{
   # QY: This is the area of code where “per server” queries can be executed and then saved back to the central database
    write-host “DBServer: ” $a.ServerName ;


    <#

    Get-ClusterGroup -Cluster $a.ServerName | 
                        Where-Object {$_.Name -like "*ag" -or $_.Name -like "*sql*"} |
                        Sort-Object -Property OwnerGroup 
   
   Get-ClusterResource -Cluster $a.ServerName |
       Where-Object {($_.OwnerGroup -like "*AG" -or $_.OwnerGroup -like "*SQL*") -and $_.ResourceType -like "*Network Name*"} |
            ft *
    #>
     $netnames = Get-ClusterResource -Cluster $a.ServerName |
        Where-Object {($_.OwnerGroup -like "*AG" -or $_.OwnerGroup -like "*SQL*") -and $_.ResourceType -like "*Network Name*"} 
        #|
         #   ft Name,RestartAction,RestartThreshold,RetryPeriodOnFailure,OwnerGroup,ResourceType,State
   
    foreach ($netname in $netnames){

        
        $netname.Name + " " +  $netname.RestartAction 

    }
#   $resource = Get-ClusterResource -Cluster $a.ServerName |
#        Where-Object {($_.OwnerGroup -like "*AG" -or $_.OwnerGroup -like "*SQL*") -and $_.ResourceType -like "*Network Name*"} ; 
#   $resource.RestartAction = 2;
    
}


#Close the connection as soon as you are done with it

$ServerAConnection.Close()