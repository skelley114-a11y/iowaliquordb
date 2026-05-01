# loads the appropriate libraries
import shutil
import urllib.request, urllib.parse, urllib.error
import re
import csv
from bs4 import BeautifulSoup
import sqlite3

def regexp(expr, item):
    if item is None:
        return False
    return re.search(expr, item, re.IGNORECASE) is not None

url="https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv?accessType=DOWNLOAD"
local_filename="iowa_liquor_sales.csv"
header= {
    'User-Agent': 'IowaliquorDBmanager (skelley114@gmail.com)',
    'X-App-Token': 'fgL74Y5CAv1nz7oYbytGRgzli'
}

with requests.get(url, headers=header, stream=True) as r:
    r.raise_for_status()
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            f.write(chunk)

conn = sqlite3.connect('liquor_sales.db')
cur = conn.cursor()


cur.executescript('''
CREATE TABLE sales_fact (
    invoice INTEGER PRIMARY KEY UNIQUE NOT NULL,
    date INTEGER NOT NULL,
    store_number INTEGER NOT NULL,
    category INTEGER,
    vendor_number INTEGER,
    brand INTEGER NOT NULL,
    pack INTEGER NOT NULL,
    bottle_volume_ml INTEGER NOT NULL,
    state_bottle_cost INTEGER NOT NULL,
    state_bottle_retail INTEGER NOT NULL,
    bottles_sold INTEGER NOT NULL,
    sale_dollars INTEGER NOT NULL,
    volume_liters INTEGER NOT NULL,
    volume_gallons INTEGER NOT NULL
);
CREATE TABLE brand_info(
    brand_id INTEGER PRIMARY KEY NOT NULL,
    pattern TEXT NOT NULL,
    brand_name TEXT NOT NULL,
);
CREATE TABLE category_info(
    category_id INTEGER PRIMARY KEY NOT NULL,
    category_name TEXT NOT NULL,
);
CREATE TABLE city_name(
    city_id INTEGER PRIMARY KEY NOT NULL,
    city_name TEXT NOT NULL,
);
CREATE TABLE zip_code(
    zip_code_id INTEGER PRIMARY KEY NOT NULL,
    zip_code INTEGER NOT NULL,
);
CREATE TABLE county_name(
    county_id INTEGER PRIMARY KEY NOT NULL,
    county_number INTEGER NOT NULL,
    county_name TEXT NOT NULL,
);
CREATE TABLE vendor_info(
    vendor_id INTEGER PRIMARY KEY NOT NULL,
    vendor_name TEXT NOT NULL,
    vendor_number INTEGER NOT NULL,
);
CREATE TABLE bottle_volume(
    bottle_volume_id INTEGER PRIMARY KEY NOT NULL,
    bottle_volume_ml INTEGER NOT NULL,
);
CREATE TABLE flavor_promo(
    flavor_promo_id INTEGER PRIMARY KEY NOT NULL,
    flavor_promo_name TEXT NOT NULL,
    brand_id INTEGER NOT NULL,
);
CREATE TABLE store_info(
    store_id INTEGER PRIMARY KRY NOT NULL,
    franchise_id INTEGER NOT NULL,
    store_name TEXT NOT NULL,
    store_number INTEGER NOT NULL,
    store_address TEXT NOT NULL,
    city_id INTEGER NOT NULL,
    zip_code_id INTEGER NOT NULL,
    county_id INTEGER NOT NULL
);
''')