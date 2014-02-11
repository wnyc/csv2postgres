import csv
import datetime
from StringIO import StringIO
import re
import psycopg2
import sys
import itertools 
import math

RE_INT = re.compile('^[+-]?[0-9]+([.]0+)?$')

def could_be_int2(items):
    return all(map(RE_INT.match, items)) and all(-(2 ** 15) <  integer(x) <  2**15 for x in items)

def could_be_int4(items):
    return all(map(RE_INT.match, items)) and all(-(2 ** 31) <  integer(x) <  2**31 for x in items)

def could_be_int8(items):
    return all(map(RE_INT.match, items)) and all(-(2 ** 63) <  integer(x) <  2**63 for x in items)

def integer(i):
    return int(i.split('.', 1)[0])
    
    
RE_FLOAT = re.compile('^[+-]?[0-9]+([.][0-9]+)?$')

def could_be_float(items):
    return all(map(RE_FLOAT.match, items))

def same_size_text(items):
    s = set()
    map(s.add, map(len, items))
    if len(s) == 1:
        return list(s)[0]
    return False

RE_AMERICAN_DATE = re.compile('^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$')
def could_be_american_date(items):
    if not all(map(RE_AMERICAN_DATE.match, items)):
        return False
    months = set()
    days = set()
    years = set()
    for item in items:
        month, day, year = map(int, item.split('/'))
        months.add(month)
        days.add(day)
        years.add(year)

    if max(months) > 12:
        return False
    if max(days) > 31:
        return False
    return True 

def american_date(date):
    month, day, year = map(int, date.split('/'))
    return datetime.date(year=year, month=month, day=day)

# 7/9/2008 10:30:00 PM
RE_AMERICAN_DATETIME = re.compile('^([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})[ ]([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})[ ]([AP]M)$')

def american_datetime(date):
    match = RE_AMERICAN_DATETIME.match(date)
    if match:
        month, day, year, hour, minute, second, apm = match.groups()
        month, day, year, hour, minute, second = map(int, (month, day, year, hour, minute, second))
        if apm == 'PM' and hour <= 12:
            hour += 12 
        try:
            return datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
        except ValueError:
            return None

def most(items):
    items = list(items)
    return len([i for i in items if not i]) < math.sqrt(len(items))
    

def could_be_american_datetime(dates):
    return most(map(american_datetime, dates))

    
def analyize(csv_data):
    values = {}
    reader = csv.DictReader(StringIO(csv_data))
    for row in reader:
        for k, v in row.items():
            values.setdefault(k, set()).add(v)

    for field in reader.fieldnames:

        if could_be_int2(values[field]):
            yield field, 'int2', integer
            continue 

        if could_be_int4(values[field]):
            yield field, 'int4', integer
            continue 

        if could_be_int8(values[field]):
            yield field, 'int8', integer
            continue 

        if could_be_float(values[field]):
            yield field, 'float', float
            continue 
        
        if could_be_american_date(values[field]):
            yield field, 'date', american_date
            continue 

        if could_be_american_datetime(values[field]):
            yield field, 'timestamp', american_datetime
            continue 

        size = same_size_text(values[field])
        if size:
            yield field, 'char(%d)' % size, str
            continue 

        yield field, 'text', str

def create_table(cxn, table, fields):
    cursor = cxn.cursor()
    sql = []
    for name, tipe, func in fields:
        chunk = "%s %s" % (name, tipe)
        sql.append(chunk)
    fields  = ", ".join(sql)
    sql = "drop table if exists %s;" % (table,)
    cursor.execute(sql);
    sql = "create table %s ( %s );" % (table, fields)
    cursor.execute(sql);

def load_tables(cxn, table, fields, data):
    reader = csv.DictReader(StringIO(data))
    field_names = ', '.join([x[0] for x in fields])
    values = ','.join(['%s'] *len( fields))
    sql = "insert into %s (%s) values (%s);" % (table, field_names, values)
    # print sql
    def process_row(datum):
        retval = []
        for name, tipe, f in fields:
            retval.append(f(datum[name]))
        return retval
    c = cxn.cursor()
    print sql
    c.executemany(sql, itertools.imap(process_row, reader))

    cxn.commit()
    # print map(process_row, reader)

    

def run():
    connection_string, table = sys.argv[1:]
    data = sys.stdin.read()
    fields = list(analyize(data))

    cxn = psycopg2.connect(connection_string)
    create_table(cxn, table, fields)
    load_tables(cxn, table, fields, data)
    
run()
