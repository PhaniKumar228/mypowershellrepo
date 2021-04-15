function get-wmiinfo ([string]$Servername) 
{
$y = $Servername
import-module sqlpsx
$ServerRepository = "EMDWProd"
$DatabaseRepository = "EMDW"

if($Servername.contains("\"))
{
$len = $Servername.length
$truncindex = $Servername.indexof("\")
$Hostname = $Servername.remove($truncindex,$len-$truncindex)
$x = $len-$truncindex
$Instance = $Servername.substring($truncindex+1,$x-1)

$servername = $Hostname
}
$X = gwmi -query "select * from
Win32_ComputerSystem" -computername $servername | select Name,
Model, Manufacturer, Description, DNSHostName,
Domain, DomainRole, PartOfDomain, NumberOfProcessors,
SystemType, TotalPhysicalMemory, UserName, Workgroup  -ErrorAction SilentlyContinue



$Model = $x.Model.tostring()
$Manufacturer = $x.Manufacturer.tostring()
$Description = $x.Description.tostring()
$y
$Qry = "Insert into ServerWMIInfo (Servername,Model,Manufacturer) values ( '$y','$Model','$Manufacturer') "
$qry
 Set-SqlData -sqlserver $ServerRepository -dbname $DatabaseRepository -qry $Qry



}