#Set connection for CMS and open------------------------------{{{
# gc .\DRDatapull.ps1 | ? {$_ -notlike '#*'} | clip.exe
$scriptpath = $MyInvocation.MyCommand.Path
$dir = Split-Path $scriptpath
$connInsert = New-Object System.Data.SqlClient.SqlConnection("Data Source=EMDWProd; Initial Catalog=eMDW; Integrated Security=SSPI")
$connInsert.Open()
#}}}
#Function to Retreive data from database ------------------------------{{{
function exec-query( $sql,$parameters=@{},$conn,$timeout=30,[switch]$help){
 if ($help){
 $msg = @"
Execute a sql statement.  Parameters are allowed.
Input parameters should be a dictionary of parameter names and values.
Return value will usually be a list of datarows.
"@
 write-Log $msg
 return
 }
 $cmd=new-object system.Data.SqlClient.SqlCommand($sql,$conn)
 $cmd.CommandTimeout=$timeout
 foreach($p in $parameters.Keys){
 [Void] $cmd.Parameters.AddWithValue("@$p",$parameters[$p])
 }
 $ds=New-Object system.Data.DataSet
 $da=New-Object system.Data.SqlClient.SqlDataAdapter($cmd)
 $da.fill($ds) | Out-Null

 return $ds
}
#}}}
#Get List of Servers to be queried for the LS Info------------------------------{{{
$Servers = exec-query "exec spDRD_GetServerList 'LSPrimary'" -conn $connInsert
#}}}
#Function to write Primary Server data to database ------------------------------{{{
function Insert-PLSData($PrimaryServer, $Database_name, $recovery_model_desc, $secondary_database, $secondary_server, $Fullbackup, $Logbackup, $diffbackup, $FullbackupLatency, $LogbackupLatency, $diffbackupLatency, $Fullbackupfile, $Tlogbackupfile, $Diffbackupfile, $Database_status, $Mirroring_Guid, $group_database_id, $CaptureDate){
  $cmdInsert = $connInsert.CreateCommand()
  $cmdInsert.CommandText =  "INSERT into DRDPrimaryServer (PrimaryServer, Database_name, recovery_model_desc, secondary_database, secondary_server, Fullbackup, Logbackup, diffbackup, FullbackupLatency, LogbackupLatency, diffbackupLatency, Fullbackupfile, Tlogbackupfile, Diffbackupfile, Database_status, Mirroring_Guid, group_database_id, CaptureDate)
                            VALUES ('$PrimaryServer', '$Database_name', '$recovery_model_desc', '$secondary_database', '$secondary_server', '$Fullbackup', '$Logbackup', '$diffbackup', '$FullbackupLatency', '$LogbackupLatency', '$diffbackupLatency', '$Fullbackupfile', '$Tlogbackupfile', '$Diffbackupfile', '$Database_status', '$Mirroring_Guid', '$group_database_id', '$CaptureDate')"
  $cmdInsert.ExecuteNonQuery() | out-Null
}
#}}}
#Function to write secondary Server data to database ------------------------------{{{
function Insert-SLSData($servername, $dbname, $last_restored_file, $last_restored_date, $backup_finish_date, $DRLatency, $RestoreLatency, $CaptureDate){
  $cmdInsert = $connInsert.CreateCommand()
    $cmdInsert.CommandText =  "INSERT into DRDSecondaryServer (servername, dbname, last_restored_file, last_restored_date, backup_finish_date, DRLatency, RestoreLatency, CaptureDate) VALUES ('$servername', '$dbname', '$last_restored_file', '$last_restored_date', '$backup_finish_date', '$DRLatency', '$RestoreLatency', '$CaptureDate')"
  $cmdInsert.ExecuteNonQuery() | out-Null
}
#}}}
#Function to write log to database ------------------------------{{{
function Insert-Log($servername,$functionname,$exception){
  $cmdInsert = $connInsert.CreateCommand()
  $cmdInsert.CommandText =  "INSERT into DRDLSLOG (ComputerName,FunctionName, Exception) VALUES ('$servername','$FunctionName','$exception')"
  $cmdInsert.ExecuteNonQuery() | out-Null
}
#}}}
#Function to Write to host ------------------------------{{{
function write-Log($Line){
    if ($host.Name -eq "ConsoleHost")
    {
        write-host $line
    }
}
#}}}
#Function to Get Primary Log Shipping information ------------------------------{{{
function Get-PrimaryLSInfo($servername){
    $pquery = gc "$dir\GetLSPrimaryInfo.sql" | out-string #out-string is important otherwise gc strips the linefeed
    #$pquery = "select * from sys.databases"
    $conls = New-Object System.Data.SqlClient.SqlConnection("Data Source=$servername; Initial Catalog=GMODBA; Integrated Security=SSPI")
    $conls.Open()
    trap{
       insert-Log $servername 'Get-PrimaryLSInfo' $_.Exception.ToString().Replace("'","''")
       continue;
    }exec-query $pquery -conn $conls
    $conls.Close()
}
#}}}
#Function to Get Secondary Log Shipping information ------------------------------{{{
function Get-SecondaryLSInfo($servername){
    $squery = gc "$dir\GetLSSecondaryInfo.sql" | out-string #out-string is important otherwise gc strips the linefeed
    $conls = New-Object System.Data.SqlClient.SqlConnection("Data Source=$servername; Initial Catalog=GMODBA; Integrated Security=SSPI")
    $conls.Open()
    trap{
       insert-Log $servername 'Get-SecondaryLSInfo' $_.Exception.ToString().Replace("'","''")
       continue;
    }exec-query $squery -conn $conls
    $conls.Close()
}
#}}}
#Call Primary LS info function for all servers in the list--------------------------------------------{{{
#$servers.Tables[0] | % { $_.server_name}
$primaryserverinfo = $servers.Tables[0] | foreach {write-Log $_.InstanceName; Get-PrimaryLSInfo $_.InstanceName} 
# Get-drives BOSSQLPRD26B
#}}}
#Insert Log shipping Primary data to database tables--------------------------------------------{{{
foreach($lspsinfo in $primaryserverinfo){
       trap{
            insert-Log $servername 'Insert LSPInfo to eMDW' $_.Exception.ToString().Replace("'","''")
            continue;
    }$lspsinfo.Tables[0] | foreach { Insert-PLSData $_.PrimaryServer $_.Database_name $_.recovery_model_desc $_.secondary_database $_.secondary_server $_.Fullbackup $_.Logbackup $_.diffbackup $_.FullbackupLatency $_.LogbackupLatency $_.diffbackupLatency $_.Fullbackupfile $_.Tlogbackupfile $_.Diffbackupfile $_.Database_status $_.Mirroring_Guid $_.group_database_id $_.CaptureDate}
}
#}}}
#Get List of Secondary Servers to be queried for the LS Info------------------------------{{{
$Servers = exec-query "exec spDRD_GetServerList 'LSSecondary'" -conn $connInsert
#}}}
#Call Primary LS info function for all servers in the list--------------------------------------------{{{
#$servers.Tables[0] | % { $_.server_name}
$Secondaryserverinfo = $servers.Tables[0] | foreach {write-Log $_.InstanceName; Get-SecondaryLSInfo $_.InstanceName} 
# Get-drives BOSSQLPRD26B
#}}}
#Insert Log shipping secondarydata to database tables--------------------------------------------{{{
foreach($lsssinfo in $Secondaryserverinfo){
    trap{
            insert-Log $servername 'Insert LSSInfo to eMDW' $_.Exception.ToString().Replace("'","''")
            continue;
    }$lsssinfo.Tables[0] | foreach { Insert-SLSData $_.servername  $_.dbname  $_.last_restored_file  $_.last_restored_date  $_.backup_finish_date  $_.DRLatency  $_.RestoreLatency  $_.CaptureDate }
}
#}}}
$connInsert.Close()
