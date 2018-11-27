import pymysql

def delete_data(data_base,table,col_name,value):
    host = 'localhost'
    username = 'root'
    password = '!QAZ2wsx'
    db_name = data_base

    try:
        connection = pymysql.connect(host=host,
                                     user=username,
                                     password=password,
                                     charset='utf8mb4',
                                     db=db_name)
        with connection.cursor() as cursor:
            sql = f"delete from {table} where {col_name} = '{value}'"
            cursor.execute(sql)
        connection.commit()
        connection.close()
    except:
        print('无需清除,跳过...')