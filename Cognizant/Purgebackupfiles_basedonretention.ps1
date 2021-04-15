param([string] $dbservername)

# Load Smo and referenced assemblies.
[void][System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.ConnectionInfo');
[void][System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.Management.Sdk.Sfc');
[void][System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO');
# Requiered for SQL Server 2008 (SMO 10.0).
[void][System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMOExtended');


$server =  New-Object Microsoft.SqlServer.Management.Smo.Server "EMDWProd";
$db = $server.Databases.Item("SQLBackupRepository");
$ExecutionLog = "G:\BackupPurgeLogs\PurgeBackupFiles_BasedOnRetention.Log"

Write-Output "Files Deleted on : "  | Add-Content $ExecutionLog 
Get-Date -Format g | Add-Content $ExecutionLog 

[String] $sql = "WITH SERVERMAIN_CTE (ServerName, DatabaseName, BackupStartDate, FileName, RetentionPeriod )
					AS
					(
						SELECT B.Server_Name, B.Database_Name, B.backup_start_date, F.physical_device_name,
						Retention_Period =  
										CASE 
											WHEN M.RetentionPeriod IS NULL THEN (select RetentionPeriod from DB_ServerBackupRetention where ServerName = 'SYSTEMDEFAULT')
											WHEN M.RetentionPeriod IS NOT NULL and M.DatabaseName = 'ALL' THEN (select RetentionPeriod from DB_ServerBackupRetention where ServerName = B.server_name)
											ELSE M.RetentionPeriod
										END
						FROM [dbo].[DB_ServerBackupRetention] AS M
						RIGHT OUTER JOIN
						[dbo].[backupset] AS B
						ON M.ServerName = B.Server_Name and (M.DatabaseName = B.database_name or M.DatabaseName = 'ALL')
						INNER JOIN
						[dbo].[backupmediafamily] AS F
						ON B.Server_Name = F.Server_Name 
						AND B.media_set_id = F.media_set_id
						AND B.Server_Name like '%PRD%'
						AND B.backup_start_date >= GETDATE()- 25
					  )
					Select FileName from SERVERMAIN_CTE WITH (NOLOCK) where BackupStartDate < GETDATE() - RetentionPeriod
					order by servername, databasename, BackupStartDate;";


$result = $db.ExecuteWithResults($sql);
$table = $result.Tables[0];

foreach ($row in $table)
{
	##Write-Output $row.Item("Filename"); 
	
	if((Test-Path -Path $row.Item("Filename")))
	  {
 	
	   Remove-Item $row.Item("Filename") -Force;
	   Write-Output $row.Item("Filename") | Add-Content $ExecutionLog 
	  }
	 else
	  {
	  	##Write-Output "Error Deleteing File" | Add-Content $ExecutionLog; 
	  }

} 



