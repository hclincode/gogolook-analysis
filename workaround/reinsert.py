import sys
import os
import mysql.connector

mysql_config = {
    'host': 'localhost',
    'database': 'gogolook',
    'user': os.environ['MYSQL_USER'],
    'password': os.environ['MYSQL_PASSWORD'],
}

SQL_INSERT_IGNORE = '''INSERT IGNORE INTO report_event
(user_id, ts)
VALUES (%s, %s)
'''

if __name__ == '__main__':
    filename = sys.argv[1]
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()
    with open(filename) as data:
        for line in data:
            line = line.strip()
            toks = line.split("Deadlock found when trying to get lock; try restarting transaction] ")
            if len(toks) != 2:
                print line
                continue

            # risk just workaround
            batch_buffer = eval(toks[1])

            try:
                cursor.executemany(SQL_INSERT_IGNORE, batch_buffer)
            except Exception as e:
                print '[DB][%s]' % e, batch_buffer
            cnx.commit()

    cnx.commit()
    cnx.close()