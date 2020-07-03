
# preparation
## mysql
- using docker
- poc and operate mysql by MySQLWorkbench
## pyhton connector
pip install mysql-connector-python

# task steps
1. python insert_data.py data/8c0cb758-5cbd-4ffc-a267-ef6c5c94cefc.csv &>  log/insert.log
2. python create_view.py
3. python query.py

# Task Introduction
## Design table schema for MySQL
### table report_event
columns:  
- user_id: int
- ts: timestamp  

primary_key (user_id, ts)  

The design keep original and simple formate of row data for flexible query in the future.  

### view continuous_report
columns:
- user_id: int
- start_date: date
- end_date: date

  

## Get number of user who reported by each day
  

## Calculate the ratio of user who continously report by [2, 5, 7, 14, 30] days
Here are two ambiguous points:  
- There is no clearly target to be compared with user who continously reported.  
i.e. It's hard to calculate the ratio. I assumed the target is all users in the data.
- The continuous day could be exactly equal to or more than the given day. I assumed the criteria is more or equal to the given day.
