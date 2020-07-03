import mysql.connector
import os

mysql_config = {
    'host': 'localhost',
    'database': 'gogolook',
    'user': os.environ['MYSQL_USER'],
    'password': os.environ['MYSQL_PASSWORD'],
}

sql_num_of_uniq_user = '''SELECT COUNT(DISTINCT user_id) FROM report_event'''

sql_num_of_user_on_date = '''SELECT COUNT(DISTINCT user_id) as num_of_user, date(ts) as report_date
	FROM report_event
    GROUP BY date(ts)'''

sql_num_of_user_who_report_continuous_n_day = '''SELECT count(distinct user_id)
FROM continuous_report
WHERE date_add(start_date, INTERVAL %s day) <= end_date'''

if __name__ == '__main__':
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()
    cnx.close()
