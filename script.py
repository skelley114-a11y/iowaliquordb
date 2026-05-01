# loads the appropriate libraries
import shutil
import urllib.request, urllib.parse, urllib.error
import re
import csv
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime, timedelta
# allows me to use regular expressions to search
def regexp(expr, item):
    if item is None:
        return False
    return re.search(expr, item, re.IGNORECASE) is not None
def regexp_extract(expr, item):
    if item is None:return None
    match = re.search(expr, item, re.IGNORECASE)
    if match:
        return item[match.end():].strip()
    return None
connection.create_function("regexp_extract", 2, regexp_extract)
def build_date_table(db_path, start_date, num_of_days):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS date(
            date_id INTEGER PRIMARY KEY NOT NULL,
            date TEXT NOT NULL,
            day_of_month INTEGER NOT NULL,
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL,
            week_number INTEGER NOT NULL
            )
        ''')
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    date_data = []
    for day in range(num_of_days):
        current = start_date + timedelta(days=day)
        date_id = 0+day
        row = (
            date_id,
            current.strftime('%Y-%m-%d'),
            current.day,
            current.month,
            current.year,
            current.isoweekday(),
            current.isocalendar()[1]
        )
        date_data.append(row)
    cursor.executemany(
        "INSERT OR IGNORE INTO date VALUES (?,?,?,?,?,?,?)",date_data
    )
    conn.commit()
    conn.close()

url="https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv?accessType=DOWNLOAD"
url2="https://raw.githubusercontent.com/skelley114-a11y/iowaliquordb/refs/heads/main/brand.txt"
url3="https://raw.githubusercontent.com/skelley114-a11y/iowaliquordb/refs/heads/main/franchise.txt"
local_filename="iowa_liquor_sales.csv"
local_brands="brands.txt"
local_franchise="franchises.txt"
# usage of tickets for faster and more consistent requests
header= {
    'User-Agent': 'IowaliquorDBmanager (skelley114@gmail.com)',
    'X-App-Token': 'fgL74Y5CAv1nz7oYbytGRgzli'
}
# connects to and downloads the necessary documents
with requests.get(url, headers=header, stream=True) as r:
    r.raise_for_status()
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            f.write(chunk)

with requests.get(url2, headers=header, stream=True) as r2:
    r2.raise_for_status()
    with open(local_brands, 'wb') as f:
        for chunk2 in r2.iter_content(chunk_size=1024):
            f.write(chunk2)

with requests.get(url3, headers=header, stream=True) as r3:
    r3.raise_for_status()
    with open(local_franchise, 'wb') as f:
        for chunk3 in r3.iter_content(chunk_size=1024):
            f.write(chunk3)

# connects to the database
conn = sqlite3.connect('liquor_sales.db')
cur = conn.cursor()

# creates the tables and keys for the database
cur.executescript('''
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS date(
    date_id INTEGER PRIMARY KEY NOT NULL,
    date TEXT PRIMARY KEY NOT NULL
);
CREATE TABLE IF NOT EXISTS brand_info(
    brand_id INTEGER PRIMARY KEY NOT NULL,
    pattern TEXT UNIQUE NOT NULL,
    brand_name TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS category_info(
    category_id INTEGER PRIMARY KEY NOT NULL,
    category_name TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS city_name(
    city_id INTEGER PRIMARY KEY NOT NULL,
    city_name TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS zip_code(
    zip_code_id INTEGER PRIMARY KEY NOT NULL,
    zip_code INTEGER UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS county_info(
    county_number INTEGER NOT NULL,
    county_name TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS vendor_info(
    vendor_id INTEGER PRIMARY KEY NOT NULL,
    vendor_name TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS bottle_volume(
    bottle_volume_id INTEGER PRIMARY KEY NOT NULL,
    bottle_volume_ml UNIQUE INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS flavor_promo(
    flavor_promo_id INTEGER PRIMARY KEY NOT NULL,
    flavor_promo_name TEXT NOT NULL,
    brand_id INTEGER NOT NULL,
    FOREIGN KEY (brand_id) REFERENCES brand_info(brand_id)
);
CREATE TABLE IF NOT EXISTS franchise(
    franchise_id INTEGER PRIMARY KEY NOT NULL,
    franchise_name TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS store_info(
    store_id INTEGER PRIMARY KEY NOT NULL,
    franchise_id INTEGER NOT NULL,
    store_name TEXT NOT NULL,
    store_address TEXT NOT NULL,
    city_id INTEGER NOT NULL,
    zip_code_id INTEGER NOT NULL,
    county_id INTEGER NOT NULL,
    FOREIGN KEY (franchise_id) REFERENCES franchise(franchise_id),
    FOREIGN KEY (county_id) REFERENCES county_info(county_id),
    FOREIGN KEY (city_id) REFERENCES city_name(city_id),
    FOREIGN KEY (zip_code_id) REFERENCES zip_code(zip_code_id)
);
CREATE TABLE IF NOT EXISTS sales_fact (
    invoice INTEGER PRIMARY KEY UNIQUE NOT NULL,
    date INTEGER NOT NULL,
    store_number INTEGER NOT NULL,
    category INTEGER,
    vendor_number INTEGER,
    brand INTEGER NOT NULL,
    flavor INTEGER NOT NULL,
    pack INTEGER NOT NULL,
    bottle_volume_ml INTEGER NOT NULL,
    state_bottle_cost INTEGER NOT NULL,
    state_bottle_retail INTEGER NOT NULL,
    bottles_sold INTEGER NOT NULL,
    FOREIGN KEY (date) REFERENCES date(date_id)
    FOREIGN KEY (store_number) REFERENCES store_info(store_id),
    FOREIGN KEY (category) REFERENCES category_info(category_id),
    FOREIGN KEY (vendor_number) REFERENCES vendor_info(vendor_id),
    FOREIGN KEY (brand) REFERENCES brand_info(brand_id),
    FOREIGN KEY (flavor) REFERENCES flavor_promo(flavor_promo_id),
    FOREIGN KEY (bottle_volume_ml) REFERENCES bottle_volume(bottle_volume_id)
);
''')

conn.commit()
#creates the lookup table for brand names from the txt file taken from GitHub
with open(local_brands, "r") as brandname:
    for word in brandname:
        brands = word.split(",")
        for brandsclean in brands:
            nameclean = brandsclean.strip()
            if not nameclean:
                continue
            name = re.escape(nameclean).replace(r"\ ", " ").replace(r"\.", ".")
            pattern = fr"""(?i)\b{name}([''"s]{{1,3}}|s[''"]?)?\b"""

            print(f'Processing: {name}')
            cur.execute('''INSERT OR IGNORE INTO brand_info 
            (pattern, brand_name) VALUES (?,?)''',
                        (pattern, name))
            cur.execute("SELECT brand_id FROM brand_info WHERE brand_name =?", (brandsclean,))
            brand_id = cur.fetchone()[0]
conn.commit()

with open(local_franchise, "r") as franchises:
    for word in franchises:
        franchises = word.split(",")
        for name in franchises:
            cleanname = name.strip()
            if not cleanname    :
                continue
            print(f'Processing: {cleanname}')
            cur.execute('''INSERT OR IGNORE INTO franchise (franchise_name) VALUES (?)''', (cleanname,))
            cur.execute('''SELECT franchise_id FROM franchise WHERE franchise_name =?''', (cleanname,))
            franchise_id = cur.fetchone()[0]

conn.commit()
# selects the columns from the previously made look-up tables in order to make the rest of the database
cur.execute('SELECT brand_id, brand_name, pattern FROM brand_info')
brand_patterns = cur.fetchall()

cur.execute("SELECT franchise_id, franchise_name FROM franchise")
all_franchises = cur.fetchall()
# populates the database with the appropriate data
with open(local_filename, mode='r', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        vendor_id = row["Vendor Number"]
        store_id = row["Store Number"]
        county_id = row["County Number"]
        description = row["Item Description"]
        found_brand_id = None
        flavor_name = description

        for brand_id, brand_name, pattern in brand_patterns:
            if re.search(pattern, description):
                found_brand_id = brand_id
                extracted_flavor = re.sub(pattern, "", description).strip()
                break
            if found_brand_id is None:
                continue
        cur.execute("INSERT OR IGNORE INTO flavor_promo (flavor_promo_name, brand_id) VALUES (?,?)", (extracted_flavor, found_brand_id))
        cur.execute("SELECT flavor_promo_id FROM flavor_promo WHERE flavor_promo_name =?", (extracted_flavor,))
        flavor_promo_id = cur.fetchone()[0]
        conn.commit()
        print("Promo flavor table done!")

        cur.execute("INSERT OR IGNORE INTO city_name (city_name) VALUES (?)", (row["City"]))
        cur.execute("SELECT city_id FROM city_name WHERE city_name =?", (row["City"]))
        city_id = cur.fetchone()[0]
        conn.commit()
        print("City name table done!")

        cur.execute("INSERT OR IGNORE INTO zip_code (zip_code_id) VALUES (?)", (row["Zip Code"]))
        cur.execute("SELECT zip_code_id FROM zip_code WHERE zip_code_id =?", (row["Zip Code"]))
        zip_id = cur.fetchone()[0]
        conn.commit()
        print("Zip code table done!")

        cur.execute("INSERT OR IGNORE INTO vendor_info (vendor_id, vendor_name) VALUES (?,?)", (vendor_id, row["Vendor Name"]))
        conn.commit()
        print("Vendor info table done!")

        cur.execute("INSERT OR IGNORE INTO county_info (county_id, county_name) VALUES (?, ?)", (county_id, row["County Name"]))
        conn.commit()
        print("County info table done!")

        cur.execute("INSERT OR IGNORE INTO bottle_volume (bottle_volume_ml) VALUES (?)", (row["Bottle Volume (ml)"]))
        cur.execute("SELECT bottle_volume_ml FROM bottle_volume WHERE bottle_volume_ml =?", (row["Bottle Volume (ml)"]))
        bottle_volume_ml = cur.fetchone()[0]
        conn.commit()
        print("Bottle volume ml table done!")

        for franchise_id, franchise_name in all_franchises:
            if re.search(franchise_id, row["Franchise Name"]):
                found_franchise_id = franchise_id
                extracted_store= re.sub(franchise_id, "", row["Franchise Name"]).strip()
                break
            if found_franchise_id is None:
                continue

        cur.execute("INSERT OR IGNORE INTO store_info (store_id, franchise_id, store_name, store_address, city_id, zip_code_id, county_id) VALUES (?,?,?,?,?,?,?)",
                    (row['Store Number'], found_franchise_id, extracted_store, row['Store Address'], city_id, zip_code_id, county_id))
        conn.commit()
        print("Store info table done!")

        cur.execute("INSERT OR IGNORE INTO sales_fact (invoice, date, store_number, category, vendor_number, brand, flavor, pack, bottle_volume_ml, state_bottle_cost, state_bottle_retail, bottles_sold) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (row['Invoice/Item Number'], date_id, store_id, category_id, vendor_id, found_brand_id, flavor_promo_id, row[Pack], bottle_volume_id, row['State Bottle Cost'], row['State Bottle Retail'], row['Bottles Sold']))
        conn.commit()
        print("Invoice table done!")
conn.close()
print('The database is now ready for you to begin your analysis!!!')