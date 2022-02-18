# Migrate data from one DB to other

## Step 1: 
Download both the .ps1 files. Right click on **_SP_5min_10min.ps1_** and click edit.

## Step 2: 
Download and install [**_MySQL Connector 8.0.28_**](https://dev.mysql.com/downloads/connector/net/8.0.html). Update Connector Directory in **Line no 8**.

## Step 3:
Input Source Database Connection Information in **Line no 14**. Save and close the file. Right Click on the file and run with PowerShell. This is a one-time activity which will create the Store Procedure in the Database.

## Step 4:
Edit the **_Scheduler_5min_10min.ps1_** file and update Connector Directory, Source Database and Destination Database information in **line 8, 11 & 12** respectively.

## Step 5:
Move the **_Scheduler_5min_10min.ps1_** script in a suitable directory to add it in OS scheduler.

## Step 6:
Open Task Scheduler and add a new task.
* Give a name to your scheduler.
* Add a trigger when you want to start the task.
* Add an action and put following input 
  - Program/Script: `powershell`
  - Add arguments: `-File [your Scheduler_5min_10min.ps1 directory]`
      i.e. `-File E:\2022\upwork\29457854\Scheduler_5min_10min.ps1`
