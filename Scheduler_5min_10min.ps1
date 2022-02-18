##DB to DB data Migrator (Daily Scheduler)
##Developer: Mehedi Hasan Chonchol
##Date: 08-02-2022
##Contact: mehedi.chonchol@gmail.com


#Locating Connector
[void][System.Reflection.Assembly]::LoadFrom(“C:\Program Files (x86)\MySQL\MySQL Connector Net 8.0.28\Assemblies\v4.8\MySql.Data.dll”)

#DB Connection String Declaration
$sourceDBConnString="server=localhost; uid=root; pwd=; database=upwork_29457854_01; Pooling=False; convert zero datetime=True"
$destDBConnString="server=localhost; uid=root; pwd=; database=test; Pooling=False; convert zero datetime=True"

##SOURCE DATA PREPARATION TASK
$sourceDB = New-Object MySql.Data.MySqlClient.MySqlConnection($sourceDBConnString)

$sourceDB.Open()
Write-Output "Source DB Connected..."

$expProcCmd = New-Object MySql.Data.MySqlClient.MySqlCommand
$expProcCmd.Connection = $sourceDB
$expProcCmd.CommandText = "CALL Export_Procedure();"

$expReader = $expProcCmd.ExecuteReader()

if($expReader.Read()){
    Write-Output "Source Procedure Execution Failed..."
    $expReader.GetString(0)
} else {    
    $expReader.Close()
    $sourceDB.Close()
    Write-Output "Source Procedure Execution Successful...! :)"
}

Write-Output "Preparing Source Data..."
$sourceDB.Open()
$selectQuery = "SELECT * FROM 5min_10min;"

$req = New-Object Mysql.Data.MysqlClient.MySqlCommand($selectQuery, $sourceDB)

$dataAdapter = New-Object MySql.Data.MySqlClient.MySqlDataAdapter($req)

$dataSet = New-Object System.Data.DataSet

#Data is stored here in Powershell DataSet
$dataAdapter.Fill($dataSet, "5min10min") | Out-Null
Write-Output "Data Preparation Successful..."

#$dataSet.Tables["5min10min"] | Export-Csv -path "E:/2022/upwork/29457854/5min_10min.csv" -NoTypeInformation
$sourceDB.Close()
Write-Output "Source DB Connection Closed...!"

##DESTINATION DATA INSERTION TASK
$destDB = New-Object MySql.Data.MySqlClient.MySqlConnection($destDBConnString)
$destDB.Open()
Write-Output "Destination DB Connected..."

#DROP & CREATE Temp Table
$emptyTempTable = New-Object MySql.Data.MySqlClient.MySqlCommand
$emptyTempTable.Connection = $destDB
$emptyTempTable.CommandText = 
    "DROP TABLE IF EXISTS 5min_10min_temp;

    CREATE TABLE `5min_10min_temp` (
        id int(11) NOT NULL AUTO_INCREMENT,
        agent_id varchar(255) DEFAULT NULL,
        over5 bigint(20) DEFAULT NULL,
        over10 bigint(20) DEFAULT NULL,
        logontime bigint(20) DEFAULT NULL,
        Date datetime DEFAULT NULL,
        PRIMARY KEY (id)
    );"


$tempReader = $emptyTempTable.ExecuteReader()

if($tempReader.Read()){
    Write-Output "Temp Table Creation Error...!"
    $tempReader.GetString(0)
} else {    
    $tempReader.Close()
}

$destDB.Close()

$destDB.Open()
#Insert Data into Temp Table
$impCmd = New-Object MySql.Data.MySqlClient.MySqlCommand
$impCmd.Connection = $destDB

#Looping into the results
Write-Output "Data Insertion Started..."
foreach($row in $dataset.Tables.Rows){

    $agent_id = $row.agent_id
    $over5 = $row.over5
    $over10 = $row.over10
    $logontime = $row.logontime
    $date = [DateTime]$row.Date
    
    $fdate = '{0:yyyy-MM-dd}' -f $date
    $agent_id = $agent_id.Replace("'", "\'")

    Write-Output $row
    Write-Output $row.Date.DateTime
        
    $insertQuery="INSERT INTO 5min_10min_temp (agent_id, over5, over10, logontime, Date)
    VALUES ('$agent_id', '$over5', '$over10', '$logontime', DATE_FORMAT('$fdate', '%Y-%m-%d'));"

    Write-Output $insertQuery

    $impCmd.CommandText = $insertQuery

    $myreader = $impCmd.ExecuteReader()

    if($myreader.Read()){
        Write-Output "DB Record Insertion Error...!"
        $myreader.GetString(0)
    } else {
        $myreader.Close()
    }
}

$destDB.Close()

$destDB.Open()
Write-Output "Data Inserted into TEMP Table..."

#INSERT into MAIN Table
Write-Output "Main Table Data INSERTION Started..."
$finalInsert = New-Object MySql.Data.MySqlClient.MySqlCommand
$finalInsert.Connection = $destDB
$finalInsert.CommandText = 
    "INSERT INTO 5min_10min(agent_id, over5, over10, logontime, Date)
    SELECT agent_id, over5, over10, logontime, DATE_FORMAT(date, '%Y-%m-%d')
    FROM 5min_10min_temp
    WHERE CONCAT(agent_id, Date) NOT IN (
	    SELECT CONCAT(agent_id, Date) FROM 5min_10min
    );"


$finalReader = $finalInsert.ExecuteReader()

if($finalReader.Read()){
    Write-Output "Main Table Data Insertion Error...!"
    $finalReader.GetString(0)
} else {    
    $finalReader.Close()
    Write-Output "DB Record Insertion Successful...:)"
}

$destDB.Close()
Write-Output "Destination DB Connection Closed...!"