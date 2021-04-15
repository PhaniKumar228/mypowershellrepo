#Create your SQL connection string, and then a connection to Wrestlers
$ServerA = "EMDWProd"
#$ServerAConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnectionString = "Data Source="+$ServerA+";Initial Catalog=eMDW;Integrated Security=SSPI;"
$ServerAConnection = new-object system.data.SqlClient.SqlConnection($ServerAConnectionString);
 
#Create a Dataset to hold the DataTable from DBServers
$dataSet = new-object "System.Data.DataSet" "DBServers"
$query = "SET NOCOUNT ON;"
        $query = $query + "`n" +"Truncate Table eMDW.dbo.DBRestoreHistory; "
        $query = $query + "`n" + "SELECT g2.name AS GroupName, g1.name AS SubGroupName, s.name AS ServerName "
        $query = $query + "`n" + "FROM [msdb].[dbo].[sysmanagement_shared_server_groups_internal] g1 "
        $query = $query + "`n" + "JOIN [msdb].[dbo].[sysmanagement_shared_server_groups_internal] g2 on g1.parent_id = g2.server_group_id "
        $query = $query + "`n" + "RIGHT OUTER JOIN msdb.dbo.sysmanagement_shared_registered_servers_internal s ON g1.server_group_id = s.server_group_id "
        $query = $query + "`n" + "left OUTER JOIN eMDW.dbo.DB_ServerMain m ON m.ServerName = s.name "
        $query = $query + "`n" + "Where g1.server_type = 0 AND g1.parent_id <> 1 "
        $query = $query + "`n" + "AND m.ActiveFlag = 'Y' AND g2.name NOT IN ('PRD','DRS', 'CLR') "
        #$query = $query + "`n" + "AND s.name in ('Bossqldev25a_2','Bossqldev07a') "   
        $query = $query + "`n" + "Order by g2.name, g1.name  "
 

# Add the SQL Snapins
Import-Module SqlPs -DisableNameChecking

#Create a DataAdapter which youll use to populate the DataSet with the results
$dataAdapter = new-object "System.Data.SqlClient.SqlDataAdapter" ($query, $ServerAConnection)
$dataAdapter.Fill($dataSet) #| Out-Null
 
$ServerAConnection.Open()

$sourceConnection  = New-Object System.Data.SqlClient.SQLConnection($SourceConnectionString)
   
$dataSet2 = new-object "System.Data.DataSet" "DBProperties" 
 foreach($a in $dataSet.Tables[0].Rows)
{
  
   # open a connection for the server/database
    $SourceConnectionString = "Data Source="+$a.Servername+";Integrated Security=SSPI;"
    #$DestinationConnectionString = "Data Source=EMDWProd;Initial Catalog=eMDW;Integrated Security=True"
    $DestinationConnectionString = "Data Source="+$ServerA+";Initial Catalog=eMDW;Integrated Security=True"
    try
    {
     
    write-host "DBServer: " $a.ServerName   ; 
 
    $tableName = "DBRestoreHistory"
    
    $sql2 = " '' AS ActualBackupSize_MB,"

    $lsQuery = "Select @@MICROSOFTVERSION/POWER(2,24) AS ProductVersion"
    $lsResults = Invoke-Sqlcmd -ServerInstance $a.ServerName -Query $lsQuery

    FOREACH ($lsVer in $lsresults)
    {       
        $PVersion = $lsVer.ProductVersion
        write-host "Version: " $PVersion
        IF ($PVersion -gt 9)
        {   
          #write-host "SQL 2008 and up"
          $sql2 =  "Case bms.is_compressed When 1 Then  Cast(compressed_backup_size/1024/1024 AS numeric(10,2)) Else Cast(bs.backup_size/1024/1024 AS numeric(10,2)) End AS ActualBackupSize_MB, "
        }
        ELSEIF ($PVersion -eq 9)
        { 
          #write-host "SQL 2005"
          $sql2 =  " Cast(bs.backup_size/1024/1024 AS numeric(10,2)) AS ActualBackupSize_MB, "
        }
    }
       
    # Construct $sql    
    $sql =  "DECLARE @dbname sysname, @days int "
    $sql = $sql + "`n" + "SET @dbname = NULL "
    $sql = $sql + "`n" + "SET @days = 30 "                       ## Collect Restore information within the past 30 days
    $sql = $sql + "`n" + "SELECT '" + $a.Servername+ "' AS ServerName, '" + $a.SubGroupName + "' AS SubEnv, '" + $a.GroupName + "' AS Env, "
    $sql = $sql + "`n" + "rsh.destination_database_name AS [RestoredAsDBName], "
    $sql = $sql + "`n" + "rsh.user_name AS [RestoredBy], "
    $sql = $sql + "`n" + "CASE WHEN rsh.restore_type = 'D' THEN 'Database' "
    $sql = $sql + "`n" + "     WHEN rsh.restore_type = 'F' THEN 'File' "
    $sql = $sql + "`n" + "     WHEN rsh.restore_type = 'G' THEN 'Filegroup' "
    $sql = $sql + "`n" + "     WHEN rsh.restore_type = 'I' THEN 'Differential' "
    $sql = $sql + "`n" + "     WHEN rsh.restore_type = 'L' THEN 'Log' "
    $sql = $sql + "`n" + "     WHEN rsh.restore_type = 'V' THEN 'Verifyonly' "
    $sql = $sql + "`n" + "     WHEN rsh.restore_type = 'R' THEN 'Revert' "
    $sql = $sql + "`n" + "     ELSE rsh.restore_type "
    $sql = $sql + "`n" + "END AS [RestoreType], "
    $sql = $sql + "`n" + "rsh.restore_date AS DateRestored, "
    $sql = $sql + "`n" + "bmf.physical_device_name AS [BackupLocation], "
    $sql = $sql + "`n" + " 1 AS OnSQLBackupShare, " #
    $sql = $sql + "`n" + "bs.backup_start_date, "
    $sql = $sql + "`n" + "bs.backup_finish_date, "
    $sql = $sql + "`n" + $sql2
    $sql = $sql + "`n" + "bs.type as BackupType, "
    $sql = $sql + "`n" + "bs.is_copy_only, "
    $sql = $sql + "`n" + "bs.database_name as BackupDBName, "
    $sql = $sql + "`n" + "bs.server_name as BackupServerName, "
    $sql = $sql + "`n" + "CASE recovery WHEN 1 THEN 'Yes'WHEN 0 THEN 'No' "
    $sql = $sql + "`n" + "END AS 'WithRecovery' "
    $sql = $sql + "`n" + ", CONVERT(varchar,GETDATE(),101) AS ToDate "
    $sql = $sql + "`n" + ", CONVERT(varchar, DATEADD (D, -@days,  GETDATE()), 101 ) AS FromDate  "  ## 30 days
    $sql = $sql + "`n" + "FROM msdb.dbo.restorehistory rsh "
    $sql = $sql + "`n" + "INNER JOIN msdb.dbo.backupset bs ON rsh.backup_set_id = bs.backup_set_id "
    $sql = $sql + "`n" + "INNER JOIN msdb.dbo.backupmediafamily bmf ON bmf.media_set_id = bs.media_set_id "
    $sql = $sql + "`n" + "inner join [msdb].[dbo].[backupmediaset] bms ON bmf.media_set_id = bms.media_set_id "
    $sql = $sql + "`n" + "WHERE DATEDIFF(D, CONVERT(varchar,restore_date,101),  CONVERT(varchar,GETDATE(),101) ) < @days " ## 30 days
    $sql = $sql + "`n" + "      AND destination_database_name = ISNULL(@dbname, destination_database_name)	"
    $sql = $sql + "`n" + "ORDER BY rsh.restore_history_id DESC "

    # write-host $sql
  
    $sourceConnection.ConnectionString = $SourceConnectionString
    $sourceConnection.open()
    $commandSourceData  = New-Object system.Data.SqlClient.SqlCommand($sql,$sourceConnection)
    #$commandSourceData .CommandTimeout = '300'

    $reader = $commandSourceData.ExecuteReader()
       
        $bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $DestinationConnectionString
        $bulkCopy.DestinationTableName = $tableName
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 0
        $bulkCopy.WriteToServer($reader)
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

# Populat eMDW..DBRestoreAnalysis table:
$lsQuery2 = "exec eMDW..sp_DBRestoreAnalysis"
Invoke-Sqlcmd -ServerInstance $ServerA -Query $lsQuery2

# Calculate Backup Restore Date Delta
$lsQuery3 = "UPDATE [eMDW].[dbo].[DBRestoreHistory]"
$lsQuery3 = $lsQuery3 + "`n" + " SET BackupRestoreDateDelta = DATEDIFF(DAY, backup_start_date, DateRestored)"
Invoke-Sqlcmd -ServerInstance $ServerA -Query $lsQuery3

#Close the connection as soon as you are done with it
$ServerAConnection.Close()