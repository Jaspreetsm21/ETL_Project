# ETL Project: From Scratch to Data Warehouse 

Aim: Setup an ETL Job to Extract daily covid data from API for Data Team, who are going to use data to create insight and dashboards. 

Result: Automated job (crontab) has been setup on EC2 (Aws) to run python daily everyday to extract, transform and load data on to Sql Table.

# Code and Resources Used
Python Version: 3.7

Packages: pandas,json,mysql,requests,pymsql,sqlalchemy and datetime.

AWs: RDs (Mysql) and EC2 (ubuntu)

# Project Process

![ ](images/etl1.PNG)

### Python Script

Inside the script, it follows the steps in the screenshot below:
- Request Api and Transform
- Connect with Mysql and load the data

![ ](images/scr1.PNG)
 
### Schedule Jobs

Scheduling Python Script using Crontab on Ec2 ( Linux)

![ ](images/cron.PNG)


### Mysql (Workbench) and Covid Data

![ ](images/sql.PNG)
