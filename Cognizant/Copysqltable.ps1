
  #requires -version 2.0  

<#
Run ps on BOSSQLDEV64 D:\  to copy "data" from dev33a to prd33a:
 
PS D:\Scripts> .\CopySQLTable -SrcServer bossqldev33a -SrcDatabase gmodba -SrcTable HAHUR.DBInventory -DestServer marinftsthur001 -DestDatabase gmodba -DestTable HAHUR.DBInventory

\\marinftsthur001\c$\HORCM\scripts\



-- ## Copy a Table Between Two SQL Server Instances
http://blogs.technet.com/b/heyscriptingguy/archive/2011/05/06/use-powershell-to-copy-a-table-between-two-sql-server-instances.aspx
QY: 7/22/2014
-- Step 1: Script out Tbls along with idx/constrains from dev33a (ZiosDev), then exec it on ZiosPrd (prd33a)
/*
SSMS 2012 -> rihgt-click "Zios" db -> tasks -> Generate scripts -> 
		-> "Choose Objects": select "Tables" and "Schemas"
		-> "Set Scripting Options": "Save to file" , click "Advanced"-> adust as needed
			note: "Types of data to script" leave default "Schema only", tried "Schema and data" & "Data only" -> always error out ( output script file is only ~800 MB, still 
errors out ), break up into a couple table at a time ( output script file is ~2 MB, still errors out when tring to F5 from my pc/dev64/sqlclt2012n1 )
*/
/* Step 2: 
Run ps on BOSSQLDEV64 D:\  to copy "data" from dev33a to prd33a:

"CopyTabl.ps1" is also available on C:\Users\Admin-QYang\Documents\GMO\PS\

PS D:\> .\CopySQLTable -SrcServer bossqldev33a -SrcDatabase Zios -SrcTable datachecks.exception_instance -DestServer bossqldev64 -DestDatabase Zios -DestTable datachecks.exception_instance

*/
#>

  Param (

      [parameter(Mandatory = $true)] 

      [string] $SrcServer,

      [parameter(Mandatory = $true)] 

      [string] $SrcDatabase,

      [parameter(Mandatory = $true)] 

      [string] $SrcTable,

      [parameter(Mandatory = $true)] 

      [string] $DestServer,

      [string] $DestDatabase, # Name of the destination database is optional. When omitted, it is set to the source database name.

      [string] $DestTable, # Name of the destination table is optional. When omitted, it is set to the source table name. 

      [switch] $Truncate # Include this switch to truncate the destination table before the copy.

  )

 

  Function ConnectionString([string] $ServerName, [string] $DbName) 

  {

    "Data Source=$ServerName;Initial Catalog=$DbName;Integrated Security=True;"

  }

 

  ########## Main body ############ 

  If ($DestDatabase.Length –eq 0) {

    $DestDatabase = $SrcDatabase

  }

 

  If ($DestTable.Length –eq 0) {

    $DestTable = $SrcTable

  }

 

  If ($Truncate) { 

    $TruncateSql = "TRUNCATE TABLE " + $DestTable

    Sqlcmd -S $DestServer -d $DestDatabase -Q $TruncateSql

  }

 

  $SrcConnStr = ConnectionString $SrcServer $SrcDatabase

  $SrcConn  = New-Object System.Data.SqlClient.SQLConnection($SrcConnStr)

  $CmdText = "SELECT * FROM " + $SrcTable

  $SqlCommand = New-Object system.Data.SqlClient.SqlCommand($CmdText, $SrcConn)  

  $SrcConn.Open()

  [System.Data.SqlClient.SqlDataReader] $SqlReader = $SqlCommand.ExecuteReader()

 

  Try

  {

    $DestConnStr = ConnectionString $DestServer $DestDatabase

    $bulkCopy = New-Object Data.SqlClient.SqlBulkCopy($DestConnStr, [System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity)

    $bulkCopy.DestinationTableName = $DestTable

    $bulkCopy.WriteToServer($sqlReader)

  }

  Catch [System.Exception]

  {

    $ex = $_.Exception

    Write-Host $ex.Message

  }

  Finally

  {

    Write-Host "Table $SrcTable in $SrcDatabase database on $SrcServer has been copied to table $DestTable in $DestDatabase database on $DestServer"

    $SqlReader.close()

    $SrcConn.Close()

    $SrcConn.Dispose()

    $bulkCopy.Close()

  }

