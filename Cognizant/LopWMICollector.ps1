
import-module sqlpsx


$ManagementInstance  = "EMDWProd"
$ManagementDb =  "eMDW"
$Qry = "SELECT  (ServerName)
        FROM [eMDW].[dbo].[DB_ServerMain]
        where ActiveFlag  = 'Y'
	    order by ServerName asc "

$ServersCollection = Get-SqlData $ManagementInstance $ManagementDb $Qry


$ServersCollection |ForEach-Object{
$server = $_
$server = $server.ServerName

#Write-Host "*************************************************************************************"
#Write-Host "The SQL server instance name is: "$server

$Servername = $server
get-wmiinfo $server  -ErrorAction SilentlyContinue

}