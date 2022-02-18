##DB to DB data Migrator (Stored Procedure)
##Developer: Mehedi Hasan Chonchol
##Date: 08-02-2022
##Contact: mehedi.chonchol@gmail.com


#Locating Connector
[void][System.Reflection.Assembly]::LoadFrom(“C:\Program Files (x86)\MySQL\MySQL Connector Net 8.0.28\Assemblies\v4.8\MySql.Data.dll”)

#DB Connection Declaration
$sourceDBConnection = New-Object MySql.Data.MySqlClient.MySqlConnection

#DB Connection String
$sourceDBConnection.ConnectionString = “server=localhost;user id=root;password=;database=upwork_29457854_01;pooling=false”

#Source Procedure Start
$sourceDBConnection.Open()
Write-Output "Database Connected..."

$exportProcedureCommand = New-Object MySql.Data.MySqlClient.MySqlCommand
$exportProcedureCommand.Connection = $sourceDBConnection

$expProcScript = "    
    DROP PROCEDURE IF EXISTS `Export_Procedure`;
    
    CREATE PROCEDURE `Export_Procedure` ()
        
    BEGIN
	    
        DROP TABLE IF EXISTS 5min_10min;

        -- Create 5min_10min TABLE in Source DB
	    CREATE TABLE `5min_10min` (
	      id int(11) NOT NULL AUTO_INCREMENT,
	      agent_id varchar(255) DEFAULT NULL,
	      over5 bigint(20) DEFAULT NULL,
	      over10 bigint(20) DEFAULT NULL,
	      logontime bigint(20) DEFAULT NULL,
	      Date datetime DEFAULT NULL,
	      PRIMARY KEY (id)
	    )
	    ENGINE = INNODB,
	    CHARACTER SET latin1,
	    COLLATE latin1_swedish_ci;

	    -- INSERT data from TWO TABLES into 5min_10min TABLE
	    INSERT INTO 5min_10min (agent_id, over5, over10, logontime, date)
	    SELECT A.agent_id
	    , CASE WHEN A.over5 IS NULL
		    THEN 0
		    ELSE A.over5
	    END AS over5
	    , CASE WHEN A.over10 IS NULL
		    THEN 0
		    ELSE A.over10
	    END AS over10
	    , B.logontime
	    , DATE_FORMAT(B.date, '%Y-%m-%d') AS date
	    FROM (
		    select lrs.agent_id, 
		    IFNULL(SUM(CASE WHEN lrs.talk_time > 300000 THEN 1 ELSE 0 END), 0) AS over5 
		    ,IFNULL(SUM(CASE WHEN lrs.talk_time > 600000 THEN 1 ELSE 0 END), 0) AS over10
		    ,FROM_UNIXTIME(start_date/1000, '%Y-%m-%d') AS date
		    FROM lnm_r_session lrs where lrs.agent_id is not null and lrs.start_date >= (unix_timestamp(current_date) - 0 * 60 * 60 * 24) * 1000 AND lrs.start_date < (unix_timestamp(current_date) - (0 - 1) * 60 * 60 * 24) * 1000 
		    AND lrs.agent_group IN ('ni', 'closer')
		    group by lrs.agent_id
	    ) A
	    RIGHT JOIN (
		    select las.agent_id, 
		    SUM(las.state_duration_time/3600000) AS logontime
		    ,FROM_UNIXTIME(state_changed_date/1000, '%Y-%m-%d') AS date
		    FROM  lnm_m_agent_status las
		    WHERE las.state_changed_date >= (unix_timestamp(current_date) - 0 * 60 * 60 * 24) * 1000 AND las.state_changed_date < (unix_timestamp(current_date) - (0 - 1) * 60 * 60 * 24) * 1000
		    AND las.agent_group IN ('ni', 'closer')
		    AND las.state IN (1, 3, 4, 5)
		    group by las.agent_id, date
	    ) B ON A.agent_id = B.agent_id AND A.date = B.date;
	    
	    COMMIT;

    END;"

$exportProcedureCommand.CommandText = $expProcScript


#Source Query Execution
Write-Output "Source Data Preparation Started..."
$myreader = $exportProcedureCommand.ExecuteReader()

if($myreader.Read()){
    Write-Output "Source Data Preparation Error..."
    $myreader.GetString(0)
} else {
    $myreader.Close()
    Write-Output "Export Procedure Stored Successfully..:)"
}

$sourceDBConnection.Close()
Write-Output "DB Connection Closed..!"