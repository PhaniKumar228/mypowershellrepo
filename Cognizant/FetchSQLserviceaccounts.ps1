 
#data table to hold results
Function out-DataTable 
{
  $dt = new-object Data.datatable  
  $First = $true  

  foreach ($item in $input){  
    $DR = $DT.NewRow()  
    $Item.PsObject.get_properties() | foreach {  
      if ($first) {  
        $Col =  new-object Data.DataColumn  
        $Col.ColumnName = $_.Name.ToString()  
        $DT.Columns.Add($Col)       }  
      if ($_.value -eq $null) {  
        $DR.Item($_.Name) = "[empty]"  
      }  
      elseif ($_.IsArray) {  
        $DR.Item($_.Name) =[string]::Join($_.value ,";")  
      }  
      else {  
        $DR.Item($_.Name) = $_.value  
      }  
    }  
    $DT.Rows.Add($DR)  
    $First = $false  
  } 

  return @(,($dt))

}

# Get sql service accounts 
Function Get-ServiceAcct ([string]$Servername )
{

 $Servername = $Servername.toUpper()                         
 $Hostname = $Servername.Replace("\INST2", "")
     
     Get-WMIObject  -ComputerName $Hostname -query "select Name, StartName, startmode from Win32_service where Name like '%SQL%' " `
     | select @{Label="ServerName";Expression={$serverName.ToUpper()}} `
        , Name `
        , @{Label="AccountName";Expression={"{0:n2}" -f($_.StartName)}} `
        , @{Label="StartMode";Expression={"{0:n2}" -f($_.StartMode)}} `
}
 

#Create SQL connection string:  connect to EMDWProd instance, eMDW database
$ServerA = "EMDWProd"   
$ServerAConnectionString = "Data Source="+$ServerA+";Initial Catalog=eMDW;Integrated Security=SSPI;"          
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
$ServerAConnection.Open()
 
 
# Get Server List & truncate table   
#  $query = "SELECT [ServerName] FROM [eMDW].[dbo].[InfoBlox_Details]  where servername = 'sqldag2012.gmo.tld' "
$query = "SET NOCOUNT ON;"
        $query = $query + "`n" +"Truncate Table eMDW.dbo.DBServrServiceAccount; "
        $query = $query + "`n" + "SELECT g2.name AS GroupName, g1.name AS SubGroupName, s.name AS ServerName "
        $query = $query + "`n" + "FROM [msdb].[dbo].[sysmanagement_shared_server_groups_internal] g1 "
        $query = $query + "`n" + "JOIN [msdb].[dbo].[sysmanagement_shared_server_groups_internal] g2 on g1.parent_id = g2.server_group_id "
        $query = $query + "`n" + "RIGHT OUTER JOIN msdb.dbo.sysmanagement_shared_registered_servers_internal s ON g1.server_group_id = s.server_group_id "
        $query = $query + "`n" + "left OUTER JOIN eMDW.dbo.DB_ServerMain m ON m.ServerName = s.name "
        $query = $query + "`n" + "Where g1.server_type = 0 AND g1.parent_id <> 1 "
        $query = $query + "`n" + "AND m.ActiveFlag = 'Y' --AND g2.name NOT IN ('PRD','DRS', 'CLR') "
        #$query = $query + "`n" + "AND s.name in ('Bossqldev25a_2','BOSSQLUAT80B\INST2') "   
        $query = $query + "`n" + "Order by g2.name, g1.name  "
    # write-host $query;

 
$dataSet = new-object "System.Data.DataSet" "DBServers"
 
$dataAdapter = new-object "System.Data.SqlClient.SqlDataAdapter" ($query, $ServerAConnection)                 
$dataAdapter.Fill($dataSet) | Out-Null
  

# the $a is one ROW from above DataSet, and you can reference specific fields with $a.fieldname from within the loop
foreach($a in $dataSet.Tables[0].Rows)
{
     write-host “DBServer: ” $a.ServerName ;
    
     $s = $a.ServerName
     Get-ServiceAcct $s
    
    $dataTable = Get-ServiceAcct $s | out-DataTable
    
    $DestinationConnectionString = "Data Source="+$ServerA+";Initial Catalog=eMDW;Integrated Security=True"
    $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $DestinationConnectionString  #$connectionString
    $bulkCopy.DestinationTableName = "dbo.DBServrServiceAccount"
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 0
    $bulkCopy.WriteToServer($dataTable)
      
}
 
#Close the connection as soon as you are done with it
$ServerAConnection.Close()