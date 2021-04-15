#$null = [appdomain]::currentdomain.getassemblies()
Add-Type -AssemblyName System.Data
$ServerAConnectionString = "Data Source=eMDWPROD;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);


 
 write-host "test";
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
$query = $query + " select distinct secondary_server, PrimaryServer from [eMDW].[dbo].[DRDPrimaryServer] where secondary_server in (select servername from [eMDW].[dbo].[DRDSecondaryServer]) and secondary_server is not null;" 


$dataAdapter = new-object "System.Data.SqlClient.SqlDataAdapter" ($query, $ServerAConnection)
$dataAdapter.Fill($dataSet) #| Out-Null
 
$ServerAConnection.Open()

echo $dataset;

#$sourceConnection  = New-Object System.Data.SqlClient.SQLConnection($SourceConnectionString)
   

$dataSet2 = new-object "System.Data.DataSet" "LSAlert" 
 foreach($a in $dataSet.Tables[0].Rows)
{
 write-host $SecondaryServer;
	$SecondaryServer = $a.secondary_server
	$PrimaryServer = $a.PrimaryServer
  write-host "DBServers: " $a.secondary_server $a.PrimaryServer; 
  
   # open a connection for the server/database
    $SourceConnectionString = "Data Source="+$a.secondary_server+";Initial Catalog=msdb;Integrated Security=SSPI;"
	$sourceConnection  = New-Object System.Data.SqlClient.SQLConnection($SourceConnectionString)
    $DestinationConnectionString = "Data Source=eMDWPROD;Initial Catalog=eMDW;Integrated Security=True;"
	try
    {
	$tableName = "LSAlert"
    
    $sql = "SELECT "
           $sql = $sql + "'$SecondaryServer' as 'Secondary_Server', "
		   $sql = $sql + "'$PrimaryServer' as 'Primary_Server', "
           $sql = $sql + "CASE WHEN j.enabled = 1 and jh.run_status = 0 THEN 'Enable/Failed' "
           $sql = $sql + "WHEN j.enabled = 1 and jh.run_status = 1 THEN 'Enable/Successful' "
           $sql = $sql + "WHEN j.enabled = 1 and jh.run_status = 2 THEN 'Enable/Retry' "
           $sql = $sql + "WHEN j.enabled = 1 and jh.run_status = 3 THEN 'Enable/Cancelled' "
           $sql = $sql + "WHEN j.enabled = 1 and jh.run_status = 4 THEN 'Enable/In Progress' "
           $sql = $sql + "WHEN j.enabled = 0 and jh.run_status = 0 THEN 'Disable/Failed' "
           $sql = $sql + "WHEN j.enabled = 0 and jh.run_status = 1 THEN 'Disable/Successful' "
		   $sql = $sql + "WHEN j.enabled = 0 and jh.run_status = 2 THEN 'Disable/Retry' "
		   $sql = $sql + "WHEN j.enabled = 0 and jh.run_status = 3 THEN 'Disable/Cancelled' "
		   $sql = $sql + "WHEN j.enabled = 0 and jh.run_status = 4 THEN 'Disable/In Progress' "
		   $sql = $sql + "END "
		   $sql = $sql + "AS [Job_Status], "
		   $sql = $sql + "(convert(datetime,rtrim(run_date)) + (run_time*9+run_time%10000*6+run_time%100*10+25*run_duration)/216e4) as 'Last_Run_Date'"
		   $sql = $sql + "FROM msdb.dbo.sysJobHistory jh full join msdb.dbo.sysJobs j "
		   $sql = $sql + "on j.job_id = jh.job_id where "
		   $sql = $sql + "jh.run_date = (select max(hi.run_date) from msdb.dbo.sysJobHistory hi where jh.job_id = hi.job_id ) and "
		   $sql = $sql + "j.name like 'LSAlert%'"
		
 
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
 
 
