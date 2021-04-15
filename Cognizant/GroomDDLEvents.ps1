#Create your SQL connection string, and then a connection to Wrestlers
$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);

#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "exec [rpt_GetServerList_DDLViewer] "
        $query = $query + "@env = 'PRD' "

# write-host $query;
#Create a DataAdapter which youll use to populate the DataSet with the results
$dataAdapter = new-object "System.Data.SqlClient.SqlDataAdapter" ($query, $ServerAConnection)
$dataAdapter.Fill($dataSet) #| Out-Null

#$ServerAConnection.Open()

$sourceConnection  = New-Object System.Data.SqlClient.SQLConnection($SourceConnectionString)
   
 foreach($a in $dataSet.Tables[0].Rows)
{
  
   # open a connection for the server/database
    $SourceConnectionString = "Data Source="+$a.Servername+";Integrated Security=SSPI;"
     try
    {
     write-host "DBServer: " $a.ServerName   ; 
       
    $sql = "USE GMODBA; Delete GMODBA.dbo.adm_DDLEvents_Log Where DATEDIFF(DAY, EventDate, getdate() ) > 90"
     
    $sourceConnection.ConnectionString = $SourceConnectionString
    $sourceConnection.open()
       
    Invoke-Sqlcmd -Query $sql -ServerInstance $sourceConnection

    }
    catch
    {
        Write-Host "There has been an error deleting from $a.ServerName"   
        
    }
$sourceConnection.close()
    
}

#Close the connection as soon as you are done with it
$ServerAConnection.Close()
