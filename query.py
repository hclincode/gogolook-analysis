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

TASK_THREE_STEP_ONE_OUTPUT_PATH = 'data/task_three_step1.csv'


def task_three_step_one(cursor):
    with open(TASK_THREE_STEP_ONE_OUTPUT_PATH, 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(('number_of_user', 'continuous_n_days'))
        for n in QUERY_CONTINUOUS_N_DAYS:
            cursor.execute(SQL_NUM_OF_USER_CONTINUOUS_REPORT_N_DAYS % (n - 1))
            for num_of_user in cursor:
                writer.writerow((num_of_user[0], n))


TASK_THREE_STEP_TWO_OUTPUT_PATH = 'data/task_three_step2.csv'


def task_three_step_two(cursor):
    cursor.execute(SQL_NUM_OF_UNIQ_USER)
    query_result = cursor.fetchone()
    if not query_result:
        print '[ERROR] no result from query'
        return

    total_user_count = query_result[0]

    task_result = []
    with open(TASK_THREE_STEP_ONE_OUTPUT_PATH, 'r') as data:
        rows = csv.reader(data)
        # skip title
        rows.next()

        for row in rows:
            number_of_user, continuous_n_days = row
            ratio_str = '%.4lf' % (float(number_of_user) / total_user_count)
            task_result.append((ratio_str, str(continuous_n_days)))

    with open(TASK_THREE_STEP_TWO_OUTPUT_PATH, 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(('ratio_of_total_user', 'continuous_n_days'))
        for ratio_str, continuous_n_days in task_result:
            writer.writerow((ratio_str, continuous_n_days))


if __name__ == '__main__':
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()

    # task_two(cursor)
    # task_three_step_one(cursor)
    # task_three_step_two(cursor)

    cnx.close()
