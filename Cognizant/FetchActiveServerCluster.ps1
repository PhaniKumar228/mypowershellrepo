# Declare all the variables needed on the script
$CMSServer = "EMDWProd";
$CMSDbName = "msdb";
$DataServer = "EMDWProd";
$DataDbName = "eMDW";
$CurrDateTime = Get-Date;

# Function to insert the data
function InsertClusterResult ($DataServerVar, $DataDbNameVar, $ServerNameVar, $IsClusteredVar, $ActiveNodeVar, $TempDBCreationTimeVar)
{
    $SqlQuery = "INSERT INTO dbo.DB_ServerActiveCluster (ServerName, IsClustered, ActiveNode, TempDBCreationDate, CollectionTime) VALUES ('$ServerNameVar', '$IsClusteredVar', '$ActiveNodeVar', '$TempDBCreationTimeVar', '$CurrDateTime')";
    Invoke-Sqlcmd -ServerInstance $DataServerVar -Database $DataDbNameVar -Query $SqlQuery;
}

# Connect to CMS database and get the list of SQL servers
$Con = new-object System.Data.SqlClient.SqlConnection("Server=$CMSServer;Database=$CMSDbName;Integrated Security=true;");
$ServerList = "SELECT DISTINCT [name] FROM [msdb].[dbo].[sysmanagement_shared_registered_servers_internal]";

$Con.Open();
$Cmd = new-object System.Data.SqlClient.SqlCommand($ServerList, $Con);
#$Cmd.CommandTimeout = 0;
$Dr = $Cmd.ExecuteReader();

# For each SQL server in the list, try to get cluster information
while($Dr.Read()) {
    $ServerName = $Dr.GetValue(0);
    Write-Output "Getting the information for $ServerName";

    if (Test-Connection -Cn $ServerName.ToLower().Replace("\inst2", "") -BufferSize 16 -Count 1 -ea 0 -quiet)
    {
        $GetClusterInfo = "SELECT @@SERVERNAME AS ServerName, CONVERT(INT, SERVERPROPERTY('IsClustered')) AS IsClustered, CONVERT(VARCHAR(255),SERVERPROPERTY('ComputerNamePhysicalNetBIOS')) AS ActiveNode, crdate as TempDBCreationDate FROM sysdatabases WHERE [name] = 'tempdb'";

        $Con2 = new-object System.Data.SqlClient.SqlConnection("Server=$ServerName;Database=master;Integrated Security=true;");
        $Con2.Open();
        $Cmd2 = new-object System.Data.SqlClient.SqlCommand($GetClusterInfo, $Con2);
        $Cmd2.CommandTimeout = 0;
        $Dr2 = $Cmd2.ExecuteReader();

        if($Dr2.HasRows)
        {
            while($Dr2.Read()) {
                #$ServerName = $Dr2.GetValue(0);
                $IsClustered = $Dr2.GetValue(1);
                $ActiveNode = $Dr2.GetValue(2);
				$TempDBCreationTime = $Dr2.GetValue(3);
                InsertClusterResult $DataServer $DataDbName $ServerName $IsClustered $ActiveNode $TempDBCreationTime;
            }
        }
        else
        {
            Write-Output "---Could not get the clustered information.";
        }

        $Dr2.Close();
        $Con2.Close();
    }
    else
    {
        Write-Output "---Server $ServerName is offline.";
    }
}

$Dr.Close();
$Con.Close();