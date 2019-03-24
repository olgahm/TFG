from mysql_helpers import BaseDMsql

# TODO: Funciones para procesar un JSON de entrada
db_connection = BaseDMsql(db_name='contratacion_del_estado', db_connector='mysql', db_server='localhost',
                          db_user='root', db_password='23091996')


def get_db_connection():
    """Init database connection

    :return: Database connection object
    """
    return db_connection



def split_array(arr, size):
    arrs = []
    while len(arr) > size:
        pice = arr[:size]
        arrs.append(pice)
        arr = arr[size:]
    arrs.append(arr)
    return arrs
