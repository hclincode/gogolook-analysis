create database gogolook;
use gogolook;
create table report_event (
user_id int, 
ts datetime,
PRIMARY KEY(user_id, ts)
);
describe report_event;

-- test data
INSERT into report_event (user_id, ts) VALUES ("37093","2020-01-03 12:42:30.000");
INSERT into report_event (user_id, ts) VALUES ("37093","2020-01-04 12:42:30.000");
INSERT into report_event (user_id, ts) VALUES ("37093","2020-01-05 12:42:30.000");
INSERT into report_event (user_id, ts) VALUES ("37094","2020-01-05 12:42:30.000");
INSERT into report_event (user_id, ts) VALUES ("37094","2020-01-06 12:42:30.000");
INSERT into report_event (user_id, ts) VALUES ("37094","2020-01-07 12:42:30.000");
INSERT into report_event (user_id, ts) VALUES ("37094","2020-01-08 12:42:30.000");
INSERT into report_event (user_id, ts) VALUES ("37094","2020-01-09 12:42:30.000");
select *, DATE(ts) from report_event limit 100;
select count(*) from report_event;
truncate table report_event;

-- query number of user on date
SELECT COUNT(DISTINCT user_id) as num_of_user, date(ts) as report_date
	FROM report_event
    GROUP BY date(ts);

-- continuous date
select r1.user_id, r1.ts from
	report_event r1,
	report_event r2
    WHERE
		r1.user_id = r2.user_id
        AND date(r1.ts) = date_add(date(r2.ts), interval -1 day);

-- continuous report event
-- ref: [https://leetcode.com/problems/find-the-start-and-end-number-of-continuous-ranges/discuss/455931/my-simple-MS-SQL-Solution]
SELECT user_id, min(ts) as start_ts, max(ts) as end_ts
FROM
	(SELECT user_id, date(ts) as ts, ROW_NUMBER() OVER(ORDER BY ts) as num FROM report_event) a
GROUP BY user_id, date_add(date(ts), interval -num day);

-- create view
drop view continuous_report;
CREATE VIEW continuous_report AS
        (SELECT user_id, min(report_date) as start_date, max(report_date) as end_date
            FROM
	        (SELECT user_id, date(ts) as report_date, ROW_NUMBER() OVER(ORDER BY ts) as num FROM report_event) as a
        GROUP BY user_id, date_add(report_date, interval -num day));
describe continuous_report;
select count(user_id) from continuous_report;