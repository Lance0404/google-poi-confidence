import duckdb
import csv
import gzip
import shutil

from duckdb import DuckDBPyConnection
from fuzzywuzzy import fuzz
from csv import Dialect

# ======================== Global Variables start ========================
osm_poi_file = './osm_poi.csv.gz'
google_poi_file = './google_poi.csv.gz'
google_osm_poi_matching_file = './google_osm_poi_matching.csv.gz'
osm_poi_tb = 'osm_poi'
google_poi_tb = 'google_poi'
google_osm_poi_tb = 'google_osm_poi'
out_csv = 'out.csv'
out_csv_gz = 'out.csv.gz'

# ======================== Global Variables end ========================

def get_table_schema(con: DuckDBPyConnection, table: str) -> dict:
    """
    * key = index; val = col name
    """
    dic = {}
    con.execute(f"describe {table}")
    print(f'==================== describe {table} ====================')
    all: list = con.fetchall()
    for i,v in enumerate(all):
        print(i, v)
        dic[i] = v[0]
    return dic


def get_fuzz_ratio(a: str, b: str) -> float:
    return fuzz.ratio(a.lower(), b.lower())/100

def get_confidence(t: tuple) -> tuple:
    """
    index: property
    ------------------
    0-3: original match table (remain untouched)
    4: osm name
    5: google name
    6: google address

    pseudo code
    if (osm.name exists):
        compare osm.name with google.name
    else: 
        compare the query string with google.name + google.address

    Return:
    0-3: original match table (remain untouched)
    4: confidence score
    """
    ratio: float 
    if t[4]:
        ratio = get_fuzz_ratio(t[4],t[5])
    else:
        google_address = t[5] + ', ' + t[6].lstrip('{').rstrip('}')
        # print(f'google_address {google_address}')
        ratio = get_fuzz_ratio(t[3],google_address)

    return tuple([t[i] for i in range(4)] + [ratio])

class LanceDialect(Dialect):
    delimiter      = ';' # I don't want to use default ','
    doublequote    = True
    lineterminator = '\r\n'
    quotechar      = '"'
    quoting        = csv.QUOTE_MINIMAL

csv.register_dialect('lance', LanceDialect)    

if __name__ == '__main__':
        
    con = duckdb.connect(database=':memory:')
    con.execute(f"CREATE TABLE {osm_poi_tb} AS SELECT * FROM read_csv_auto('{osm_poi_file}')")
    con.execute(f"CREATE TABLE {google_poi_tb} AS SELECT * FROM read_csv_auto('{google_poi_file}')")
    con.execute(f"CREATE TABLE {google_osm_poi_tb} AS SELECT * FROM read_csv_auto('{google_osm_poi_matching_file}')")

    # osm_dict = get_table_schema(con, osm_poi_tb) # 33 col
    # google_dict = get_table_schema(con, google_poi_tb) # 23 col
    match_dict = get_table_schema(con, google_osm_poi_tb) # 4 col

    # print(osm_dict)
    """
    col of interest: idx 
    name: 5
    address_*: 8-24
    """
    # print(google_dict)
    """
    name: 4
    """
    # print(match_dict)

    # con.execute(f"""select count(*) from {google_osm_poi_tb} as a""")
    # print(f'row count: {con.fetchone()}')

    con.execute(f"""select m.*, osm.name, goo.name, goo.address
    from {google_osm_poi_tb} as m, {osm_poi_tb} as osm, {google_poi_tb} as goo
    where m.osm_id  = osm.osm_id 
    and m.internal_id = goo.internal_id
    """)

    header = [i for i in match_dict.values()] + ['confidence_score']
    with open(out_csv, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f, dialect='lance')
        writer.writerow(header)
        for i in con.fetchall():
            j: tuple = get_confidence(i)
            writer.writerow(j)

    with open(out_csv, 'rb') as f_in:
        with gzip.open(out_csv_gz, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)