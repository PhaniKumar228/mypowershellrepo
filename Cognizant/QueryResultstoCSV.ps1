 

$query1 = "select * from eMDW.dbo.DB_IndexMissing Order by CollectionDate Desc "
$query2 = "select * from eMDW.dbo.DB_PlanCache Order by CollectionDate Desc "
$query3 = "select * from eMDW.dbo.DB_IndexUsage Order by CollectionDate Desc "
$query4 = "select * from eMDW.dbo.DB_BufferPool Order by CollectionDate Desc"
$query5 = "select * from eMDW.dbo.DB_BufferPool_RolledUp Order by CollectionDate Desc"

 
# Query the tables
$instanceName = "EMDWProd"
$results1 = Invoke-Sqlcmd -Query $query1 -ServerInstance $instanceName
$results2 = Invoke-Sqlcmd -Query $query2 -ServerInstance $instanceName
$results3 = Invoke-Sqlcmd -Query $query3 -ServerInstance $instanceName
$results4 = Invoke-Sqlcmd -Query $query4 -ServerInstance $instanceName
$results5 = Invoke-Sqlcmd -Query $query5 -ServerInstance $instanceName

# Output to CSV
write-host "Saving Query Results in CSV format..." 
#$results1 | export-csv  $csvFilePath   -NoTypeInformation
$results1 | export-csv  D:\GMOPowershell\output\DB_IndexMissing.csv   -NoTypeInformation
$results2 | export-csv  D:\GMOPowershell\output\DB_PlanCache.csv   -NoTypeInformation
$results3 | export-csv  D:\GMOPowershell\output\DB_IndexUsage.csv   -NoTypeInformation
$results4 | export-csv  D:\GMOPowershell\output\DB_BufferPool.csv   -NoTypeInformation
$results5 | export-csv  D:\GMOPowershell\output\DB_BufferPool_RolledUp.csv   -NoTypeInformation
 