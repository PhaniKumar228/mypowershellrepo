

#Create your SQL connection string to connect to EMDWProd instance and eMDW database.  -- and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"          
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
#Create a Dataset to hold the DataTable from the InfoBlox_Details table (which hosts the list of all prod sql svr names)
$dataSet = new-object "System.Data.DataSet" "DBServers"
    #  $query = "SELECT [ServerName] FROM [eMDW].[dbo].[InfoBlox_Details]  where servername = 'sqldag2012.gmo.tld' "
$query = "SELECT SQLServerInstance as ServerName FROM [eMDW].[dbo].[DB_ServerPrdLicensing] ORDER BY 1  "                  

   write-host $query;
#Create a DataAdapter which we will use to populate the DataSet (like a recordset)
$dataAdapter = new-object "System.Data.SqlClient.SqlDataAdapter" ($query, $ServerAConnection)                 
$dataAdapter.Fill($dataSet) #| Out-Null
 
$ServerAConnection.Open()


$sourceConnection  = New-Object System.Data.SqlClient.SQLConnection($SourceConnectionString)
   
$dataSet2 = new-object "System.Data.DataSet" "DBProperties" 

# the $a is one row from the resultset, and you can reference specific fields with $a.fieldname from within the loop
 foreach($a in $dataSet.Tables[0].Rows)
{
   # QY: This is the area of code where “per server” queries can be executed and then saved back to the central database
    write-host “DBServer: ” $a.ServerName ;

   # open a connection for the server/database
    $SourceConnectionString = "Data Source="+$a.Servername+";Integrated Security=SSPI;"                       
    $DestinationConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=True"   
     try
    {
     write-host "DBServer: " $a.ServerName   ; 
 
     $tableName = "DB_ServerPrdLicensing_temp"                                                                 
	   
     #$sql =   "SELECT '" + $a.Servername+ "' AS ServerName, ServerProperty('IsClustered') as IsClustered, ServerProperty('ComputerNamePhysicalNetBIOS') as ActiveNode"          
      $sql =   "SELECT '" + $a.Servername+ "' AS ServerName, convert(int,ServerProperty('IsClustered')) as IsClustered, convert(nvarchar(128),ServerProperty('ComputerNamePhysicalNetBIOS')) as ActiveNode"
     
     write-host $sql
     $sourceConnection.ConnectionString = $SourceConnectionString
     $sourceConnection.open()
     $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)                

     $reader = $commandSourceData.ExecuteReader()
    
   
        $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $DestinationConnectionString                     
        $bulkCopy.DestinationTableName = $tableName
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 0
        
        $bulkCopy.ColumnMappings.Add("ServerName", "ServerName");
        $bulkCopy.ColumnMappings.Add("IsClustered", "IsClustered");
        $bulkCopy.ColumnMappings.Add("ActiveNode", "ActiveNode");

       
        $bulkCopy.WriteToServer($reader)
    }
    catch
    {
        # if there is an error in the powershell execution, catch it and return it to the screen
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