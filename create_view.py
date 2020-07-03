import mysql.connector
import os

mysql_config = {
    'host': 'localhost',
    'database': 'gogolook',
    'user': os.environ['MYSQL_USER'],
    'password': os.environ['MYSQL_PASSWORD'],
}

create_view_sql = '''
    CREATE VIEW continuous_report AS 
        (SELECT user_id, min(report_date) as start_date, max(report_date) as end_date
            FROM
	        (SELECT user_id, date(ts) as report_date, ROW_NUMBER() OVER(ORDER BY ts) as num FROM report_event) as a
        GROUP BY user_id, date_add(report_date, interval -num day));
'''

if __name__ == '__main__':
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()
    cursor.execute(create_view_sql)
    cnx.commit()
    cnx.close()
