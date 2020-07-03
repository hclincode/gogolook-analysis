import mysql.connector
import os
import csv
import sys
import re

ts_regex = r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(\.[0-9]{1,3})?'
re_ts_validate = re.compile(ts_regex)
mysql_config = {
    'host': 'localhost',
    'database': 'gogolook',
    'user': os.environ['MYSQL_USER'],
    'password': os.environ['MYSQL_PASSWORD'],
}

add_record = '''INSERT INTO report_event
(user_id, ts)
VALUES (%s, %s)
'''


def is_valid_row(user_id, ts):
    if not re_ts_validate.match(ts):
        print '[INVALID][ts]', user_id, ts
        return False
    try:
        int(user_id)
    except ValueError:
        print '[INVALID][id]', user_id, ts
        return False

    return True


if __name__ == '__main__':
    csv_file = sys.argv[1]
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()

    error_count = 0
    with open(csv_file, 'r') as data:
        rows = csv.reader(data)

        line_count = 0
        for row in rows:
            user_id, ts = row
            if not is_valid_row(user_id, ts):
                error_count += 1
                continue

            # too many redundant record (id, ts) (primary key in schema)
            # use execute instead of executemany
            try:
                cursor.execute(add_record, (user_id, ts))
            except Exception as e:
                print '[DB][%s]' % e, user_id, ts
                continue

            line_count += 1
            if line_count % 1000 == 999:
                cnx.commit()

    cnx.commit()
    cnx.close()
