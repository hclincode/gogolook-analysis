import mysql.connector
import os
import csv

mysql_config = {
    'host': 'localhost',
    'database': 'gogolook',
    'user': os.environ['MYSQL_USER'],
    'password': os.environ['MYSQL_PASSWORD'],
}

SQL_NUM_OF_UNIQ_USER = '''SELECT COUNT(DISTINCT user_id) FROM report_event'''

SQL_NUM_OF_USER_ON_DATE = '''SELECT COUNT(DISTINCT user_id) as num_of_user, date(ts) as report_date
	FROM report_event
    GROUP BY date(ts)'''

SQL_NUM_OF_USER_CONTINUOUS_REPORT_N_DAYS = '''SELECT count(distinct user_id) as number_of_user
FROM continuous_report
WHERE date_add(start_date, INTERVAL %s day) <= end_date'''


def task_two(cursor):
    with open('data/task_two.csv', 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(('num_of_user', 'report_date'))
        cursor.execute(SQL_NUM_OF_USER_ON_DATE)
        for num_of_user, report_date in cursor:
            writer.writerow((num_of_user, report_date))


QUERY_CONTINUOUS_N_DAYS = (2, 5, 7, 14, 30)


def task_three(cursor):
    with open('data/task_three.csv', 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(('number_of_user', 'continuous_n_days'))
        for n in QUERY_CONTINUOUS_N_DAYS:
            cursor.execute(SQL_NUM_OF_USER_CONTINUOUS_REPORT_N_DAYS % (n-1))
            for num_of_user in cursor:
                writer.writerow((num_of_user[0], n))


if __name__ == '__main__':
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()

    # task_two(cursor)
    #task_three(cursor)

    cnx.close()
