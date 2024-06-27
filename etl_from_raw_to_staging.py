import duckdb
from datetime import datetime

def can_be_converted_to_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def can_be_converted_to_double(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def can_be_converted_to_date(s, date_format="%Y-%m-%d"):
    try:
        datetime.strptime(s, date_format)
        return True
    except ValueError:
        return False

def can_be_converted_to_timestamp(s):
    formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]
    for fmt in formats:
        try:
            datetime.strptime(s, fmt)
            return True
        except ValueError:
            continue
    return False

conn = duckdb.connect("D:/Downloads/duckdb_cli-windows-amd64/data/mydb1.db")
c = conn.cursor()

#DATAPOINTS
c.execute("""
    SELECT D_ID, CREATED, ORIGIN_PID, DESTINATION_PID, VALID_FROM, VALID_TO, 
            COMPANY_ID, SUPPLIER_ID, EQUIPMENT_ID, RAW_PID 
    FROM raw.datapoints where STG_STATUS = 'NEW'
        """)
rows = c.fetchall()

for row in rows:
    if ( can_be_converted_to_int(row[0]) and can_be_converted_to_timestamp(row[1]) and can_be_converted_to_int(row[2]) and 
         can_be_converted_to_int(row[3]) and can_be_converted_to_date(row[4]) and can_be_converted_to_date(row[5]) and 
         can_be_converted_to_int(row[6]) and can_be_converted_to_int(row[7]) and can_be_converted_to_int(row[8]) ):
        c.execute('''UPDATE staging.datapoints
                     SET IS_ACTIVE = 0
                     WHERE IS_ACTIVE = ? AND D_ID = ?
                    ''', ('1', row[0]))
        c.execute('''INSERT INTO staging.datapoints(D_ID, CREATED, ORIGIN_PID, DESTINATION_PID, VALID_FROM, VALID_TO, 
                     COMPANY_ID, SUPPLIER_ID, EQUIPMENT_ID, IS_ACTIVE) VALUES (?,?,?,?,?,?,?,?,?,1)''', 
                  (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]))
        
        c.execute("""
        UPDATE raw.datapoints
        SET STG_STATUS = 'LOADED', STG_LOAD_TIME = current_timestamp
        WHERE STG_STATUS = ? AND RAW_PID = ?
        """, ('NEW', row[9]))
    else:
        c.execute("""
        UPDATE raw.datapoints
        SET STG_STATUS = 'ERROR', STG_LOAD_TIME = current_timestamp
        WHERE STG_STATUS = ? AND RAW_PID = ?
        """, ('NEW', row[9]))        
    
conn.commit()

#CHARGES
c.execute("""
    SELECT D_ID, CURRENCY, CHARGE_VALUE, RAW_PID 
    FROM raw.charges where STG_STATUS = 'NEW'
        """)
rows = c.fetchall()

for row in rows:
    if can_be_converted_to_int(row[0]) and can_be_converted_to_double(row[2]):
        c.execute('INSERT INTO staging.charges(D_ID, CURRENCY, CHARGE_VALUE) VALUES (?,?,?)', 
                  (row[0],row[1],row[2]))
        c.execute("""
        UPDATE raw.charges 
        SET STG_STATUS = 'LOADED', STG_LOAD_TIME = current_timestamp
        WHERE STG_STATUS = ? AND RAW_PID = ?
        """, ('NEW', row[3]))
    else:
        c.execute("""
        UPDATE raw.charges 
        SET STG_STATUS = 'ERROR', STG_LOAD_TIME = current_timestamp
        WHERE STG_STATUS = ? AND RAW_PID = ?
        """, ('NEW', row[3]))        
    
conn.commit()

#REGIONS
c.execute("""
    SELECT SLUG, NAME, PARENT, RAW_PID 
    FROM raw.regions where STG_STATUS = 'NEW'
        """)
rows = c.fetchall()

for row in rows:
    c.execute('INSERT INTO staging.regions(SLUG, NAME, PARENT) VALUES (?,?,?)', (row[0],row[1],row[2]))

    c.execute("""
    UPDATE raw.regions 
    SET STG_STATUS = 'LOADED', STG_LOAD_TIME = current_timestamp
    WHERE STG_STATUS = ? AND RAW_PID = ?
    """, ('NEW', row[3]))
    
conn.commit()

#EXCHANGE_RATES
c.execute("""
    SELECT DAY, CURRENCY, RATE, RAW_PID 
    FROM raw.exchange_rates where STG_STATUS = 'NEW'
        """)
rows = c.fetchall()

for row in rows:
    if can_be_converted_to_date(row[0]) and can_be_converted_to_double(row[2]):
        c.execute('INSERT INTO staging.exchange_rates(DAY, CURRENCY, RATE) VALUES (?,?,?)', (row[0],row[1],row[2]))
        c.execute("""
        UPDATE raw.exchange_rates 
        SET STG_STATUS = 'LOADED', STG_LOAD_TIME = current_timestamp
        WHERE STG_STATUS = ? AND RAW_PID = ?
        """, ('NEW', row[3]))
    else:
        c.execute("""
        UPDATE raw.exchange_rates 
        SET STG_STATUS = 'ERROR', STG_LOAD_TIME = current_timestamp
        WHERE STG_STATUS = ? AND RAW_PID = ?
        """, ('NEW', row[3]))        
    
conn.commit()

#PORTS
c.execute("""
    SELECT PID, CODE, SLUG, NAME, COUNTRY, COUNTRY_CODE, RAW_PID 
    FROM raw.ports where STG_STATUS = 'NEW'
        """)
rows = c.fetchall()

for row in rows:
    if can_be_converted_to_int(row[0]):
        c.execute('INSERT INTO staging.ports(PID, CODE, SLUG, NAME, COUNTRY, COUNTRY_CODE) VALUES (?,?,?,?,?,?)', 
                  (row[0],row[1],row[2],row[3],row[4],row[5]))
        c.execute("""
        UPDATE raw.ports 
        SET STG_STATUS = 'LOADED', STG_LOAD_TIME = current_timestamp
        WHERE STG_STATUS = ? AND RAW_PID = ?
        """, ('NEW', row[6]))
    else:
        c.execute("""
        UPDATE raw.ports 
        SET STG_STATUS = 'ERROR', STG_LOAD_TIME = current_timestamp
        WHERE STG_STATUS = ? AND RAW_PID = ?
        """, ('NEW', row[6]))        
    
conn.commit()


conn.close() 