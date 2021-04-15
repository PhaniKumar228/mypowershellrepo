$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=msdb;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);



$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
$query = $query + "select name from [msdb].[dbo].[sysmanagement_shared_registered_servers_internal];" 




$dataAdapter = new-object "System.Data.SqlClient.SqlDataAdapter" ($query, $ServerAConnection)
$dataAdapter.Fill($dataSet) #| Out-Null
 
$ServerAConnection.Open()


$dataSet2 = new-object "System.Data.DataSet" "LinkedServer" 
 foreach($a in $dataSet.Tables[0].Rows)
{
  write-host "DBServer: " $a.name; 
  
   # open a connection for the server/database
    $SourceConnectionString = "Data Source="+$a.name+";Initial Catalog=msdb;Integrated Security=SSPI;"
	$sourceConnection  = New-Object System.Data.SqlClient.SQLConnection($SourceConnectionString)
    $DestinationConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=True;"
	try
    {

    $tableName = "LinkedServer"
    
    
		$sql = "SELECT "
           $sql = $sql + "@@ServerName as ServerName, "
	       $sql = $sql + "servers.name AS [ServerLink], "
           $sql = $sql + "product AS [ProductName_LS], "
           $sql = $sql + "data_source AS [LSproperties], "
           $sql = $sql + "remote_name AS [Remote_UserName], "
		   $sql = $sql + "linked.local_principal_id AS [Local_Principal_ID], "
		   $sql = $sql + "linked.uses_self_credential AS [UserSelf_Credential], "
           $sql = $sql + "catalog AS [Catalog] "
		   $sql = $sql + "FROM sys.servers servers "
		   $sql = $sql + "INNER JOIN sys.linked_logins linked on servers.server_id = linked.server_id "
	
 
    $sourceConnection.ConnectionString = $SourceConnectionString
    $sourceConnection.open()
    $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
    $commandSourceData.CommandTimeout = 300

    $reader = $commandSourceData.ExecuteReader()
    
   
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
        $reader.close()
    }
    $sourceConnection.close()
    
}
#Write-Host "Write-DataTable$($connectionName):$ex.Message"

#Close the connection as soon as you are done with it

$ServerAConnection.Close()
 
 
