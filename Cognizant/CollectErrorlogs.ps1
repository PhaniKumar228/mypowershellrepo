import-module sqlpsx

$ServerRepository = "EMDWProd"

$DatabaseRepository = "EMDW"

$X =  (Get-Date).AddDAYS(-1)

#Return the servers and the last collection date from error logs

get-SqlData -sqlserver $ServerRepository -dbname $DatabaseRepository -qry "SELECT   (ServerName)
FROM         dbo.DB_ServerMain
WHERE     (ActiveFlag = 'Y') 
AND SERVERNAME IN ( SELECT SERVERNAME FROM DB_ServerGroup
WHERE GROUPNAME = 'PRD') 
ORDER BY ServerName
" | foreach {

       

       $ServerName = $_.Servername

      
       

       get-sqlserver $ServerName | foreach {    

              

              #If this is the first collection, takes the date 2010/01/01

              if ($DateLastLogErrorImported.value -eq $null -or $DateLastLogErrorImported.value -eq "")

                     { $DateLastLogErrorImported = '2010/01/01' }

                           

#Retrieve the error log from the current server in foreach. Apply a   #filter to only LogDate above and equal to Last Collection date

              #and insert into Repository

              $Error.Clear()

              Get-SqlErrorLog -sqlserver $ServerName -lognumber 0 | where-object { $_.LogDate -ge $X} | foreach {

                           

                           $Text = $($_.text) -replace "'"

                           Set-SqlData -sqlserver $ServerRepository -dbname $DatabaseRepository -qry "Insert into SQLLogInfo (Servername,LogDate,ProcessInfo,text) values ('$($ServerName)','$($_.Logdate)','$($_.ProcessInfo)','$Text)')"

              }

                     

                  
       

       }

}


