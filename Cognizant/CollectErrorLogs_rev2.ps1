
import-module sqlpsx

$ServerRepository = "EMDWProd"
$DatabaseRepository = "EMDW"

get-SqlData -sqlserver $ServerRepository -dbname $DatabaseRepository -qry "

SELECT servername ,max(logdate) as MaxDate
FROM SQLLogInfo
where servername in 
(SELECT   (ServerName)
FROM         dbo.DB_ServerMain 
WHERE     (ActiveFlag = 'Y') 

)
group by servername
union 
SELECT   (ServerName), getdate()-1
FROM         dbo.DB_ServerMain 
WHERE     (ActiveFlag = 'Y') 
AND ServerName NOT IN (SELECT servername from SQLLogInfo)

" | foreach {
$ServerName = $_.Servername
$Date  = $_.MaxDate

if ($Date -eq $null -or $Date -eq "")
 { 
  WRITE-HOST "ENTERED IF "
  Write-Host $ServerName
  $Date = '2013/07/17' 
  }

#Write-Host "Start collection for server..............................."

$ServerName
$Date

get-sqlserver $ServerName | foreach {    

$Error.Clear()
Get-SqlErrorLog -sqlserver $ServerName -lognumber 0 | where-object { $_.LogDate -ge $Date} | foreach {
$Text = $($_.text) -replace "'"
Set-SqlData -sqlserver $ServerRepository -dbname $DatabaseRepository -qry "Insert into SQLLogInfo (Servername,LogDate,ProcessInfo,text) values ('$($ServerName)','$($_.Logdate)','$($_.ProcessInfo)','$Text)')"
 }
 }
#Write-Host "end collection for server..............................."
}


