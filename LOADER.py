import gzip
import psycopg2 # type: ignore

def DataSwitch_PostgresLoad(**params):
    # We need to set the following values, either by passing it into the funtion or manually assigning them here
    DB_USER = ''
    DB_PASSWORD = ''
    DB_HOST = ''
    DB_PORT = ''
    DB_NAME = ''
    DB_SCHEMA = ''
    TBL_NM = ''
    FILEPATH = ''
    TargetLoadQuery = ''
    try:
        for key, value in params.items():
            if key == 'DB_USER':
                DB_USER = value
            elif key == 'DB_PASSWORD':
                DB_PASSWORD = value
            elif key == 'DB_HOST':
                DB_HOST = value
            elif key == 'DB_PORT':
                DB_PORT = value
            elif key == 'DB_NAME':
                DB_NAME = value
            elif key == 'DB_SCHEMA':
                DB_SCHEMA = value
            elif key == 'TBL_NM':
                TBL_NM = value
            elif key == 'FILEPATH':
                FILEPATH = value
            elif key == 'TargetLoadQuery':
                TargetLoadQuery = value
        gzFilePath = FILEPATH + '.gz'
        with open(FILEPATH, 'rb') as f_in, gzip.open(gzFilePath, 'wb') as f_out:
            f_out.writelines(f_in)
        print(f"Connecting Database...")
        ps_conn = psycopg2.connect(user=DB_USER,password=DB_PASSWORD,host=DB_HOST,port=DB_PORT,database=DB_NAME)
        cursor = ps_conn.cursor()

        # DELETE Existing records in Postgres Table
        cursor.execute('DELETE FROM ' + DB_SCHEMA + '.' + TBL_NM + ';')
        print('Clearing Target Postgres Table')
        cursor.close()

        # TargetLoadQuery must always be schemaName.tableName(column1, column2, .....) in the order of column. Example: 
        # TargetLoadQuery = "ps360.AU_CHASSIS_ITEM(ITEM_ID, CHASSIS_NUM, CHASSIS_ORDER_YEAR, CHASSIS_ORDER_DIVISION_CD, UPDATE_INTERFACE_NM, CREATE_DTTM, LAST_UPDATE_DTTM)"

        FileFormat = 'CSV'
        FileDelimiter = '|'
        FileHeaderExists = False

        sql = f"COPY {TargetLoadQuery} FROM STDIN WITH (FORMAT {FileFormat}, DELIMITER '{FileDelimiter}', HEADER {FileHeaderExists}, QUOTE '\"')"
        print(sql)
        with gzip.open(gzFilePath, 'rb') as gzip_file:
            cursor.copy_expert(sql, gzip_file)
        print(f"Data loaded successfully into table - '{TBL_NM}'")
    except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error - Database - {error}")
            isDBLoadFailed = True
    finally:
        # Closing Database Connection.
        if ps_conn:
            cursor.close()
            ps_conn.close()
            print(f"Closing DB Connection...")