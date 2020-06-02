from snowflake.connector import connect as sf_connect
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

class SnowFlakeDb(object):
    """Represent SnowFlake database"""

    def __init__(self, username=None, password=None):
        self._sf_config = {
            "account": "",
            "warehouse": "",
            "role": "",
            "user": "",
            "password": "",
            "database": "",
            "schema": "",
            "paramstyle": "numeric",
        }

    def fetchall(self, query, params=None, dict_cursor=False):
        """Fetch all using given query from SnowFlake database"""

        cursor_class = DictCursor if dict_cursor else SnowflakeCursor
        with sf_connect(**self._sf_config) as conn:
            with conn.cursor(cursor_class) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
        return results


sf_db = SnowFlakeDb()

stmt0 = "insert into user_stats_per_day "
stmt1 = "select distinct name from SNOWFLAKE.ACCOUNT_USAGE.USERS where disabled = 'false' and deleted_on is NULL;"

stmt2_part1 = "select total_elapsed_time, warehouse_name, warehouse_size, query_text,execution_status,'"
stmt2_part2 = """' as user_name  from
table(information_schema.QUERY_HISTORY_BY_USER(
  END_TIME_RANGE_START => dateadd('hours',-24,current_timestamp()), RESULT_LIMIT => 10000,
  USER_NAME => '"""
stmt2_part3 = "' )) where execution_status != 'RUNNING'"
union_part = "\n UNION \n"
stmt2  = ""

print("before query1")
result1 = sf_db.fetchall(stmt1)
n = len(result1)

for c, u in enumerate(result1):
    if c+1 == n:
        stmt2 = stmt2 + stmt2_part1 + u[0] + stmt2_part2 + u[0] + stmt2_part3
    else:
        stmt2 = stmt2 + stmt2_part1 + u[0] + stmt2_part2 + u[0] + stmt2_part3 + union_part

stmt2_layer_part1 = "select sum(total_elapsed_time)/1000 total_time,warehouse_name, warehouse_size, user_name,current_date from ( "
stmt2_layer_part2 = ") group by warehouse_name,warehouse_size,user_name having warehouse_name is not null and warehouse_size is not null; "
stmt2_layer = stmt2_layer_part1 + stmt2 + stmt2_layer_part2

print("before query2")
result2 = sf_db.fetchall(stmt0 + stmt2_layer)