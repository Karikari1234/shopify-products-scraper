import csv
import json
import urllib.request
import requests
import pandas as pd
import argparse
from bs4 import BeautifulSoup
from collections import OrderedDict

parser = argparse.ArgumentParser(description="Scrap ALL products data from Shopify store")
parser.add_argument('-t', '--target', dest='website_url', type=str, help='URL to Shopify store (https://shopifystore.com)')
parser.add_argument('-v', '--variants', dest='variants', action="store_true", help='Scrap also with variants data')
parser.add_argument('--debug', dest='debug', action="store_true", help='Show available fields for debugging')
args = parser.parse_args()

if not args.website_url:
    print("usage: shopify_scraper.py [-h] [-t WEBSITE_URL] [-v] [--debug]")
    exit(0)

base_url = args.website_url
url = base_url + '/products.json'
with_variants = args.variants

def flatten_dict(d, parent_key='', sep='_'):
    """Flatten nested dictionaries to get all data"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            # Handle list of dictionaries (like images, options)
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
                else:
                    items.append((f"{new_key}_{i}", item))
        elif isinstance(v, list):
            # Handle simple lists
            items.append((new_key, '|'.join(map(str, v)) if v else ''))
        else:
            items.append((new_key, v))
    return dict(items)

def get_all_product_fields(products_sample):
    """Analyze products to get all possible fields"""
    all_fields = set()
    for product in products_sample:
        flattened = flatten_dict(product)
        all_fields.update(flattened.keys())
    return sorted(all_fields)

def get_all_variant_fields(variants_sample):
    """Analyze variants to get all possible fields"""
    all_fields = set()
    for variant in variants_sample:
        if isinstance(variant, dict):
            flattened = flatten_dict(variant)
            all_fields.update(flattened.keys())
    return sorted(all_fields)

def get_page(page):
    try:
        data = urllib.request.urlopen(url + '?page={}'.format(page)).read()
        products = json.loads(data)['products']
        return products
    except Exception as e:
        print(f"Error fetching page {page}: {e}")
        return []

def get_tags_from_product(product_url):
    try:
        r = urllib.request.urlopen(product_url).read()
        soup = BeautifulSoup(r, "html.parser")
        
        title = soup.title.string if soup.title else ''
        description = ''
        
        meta = soup.find_all('meta')
        for tag in meta:
            if 'name' in tag.attrs.keys() and tag.attrs['name'].strip().lower() == 'description':
                description = tag.attrs['content']
                break
        
        return [title, description]
    except Exception as e:
        print(f"Error getting tags from {product_url}: {e}")
        return ['', '']

def get_complete_product_data(product_url):
    """Get complete product data including variants from JSON endpoint"""
    try:
        response = requests.get(product_url + '.json')
        response.raise_for_status()
        return response.json()['product']
    except Exception as e:
        print(f"Error getting complete product data from {product_url}: {e}")
        return None

# First, collect a sample to determine all available fields
print("[+] Analyzing available fields...")
sample_products = get_page(1)
if not sample_products:
    print("[-] No products found or error accessing the store")
    exit(1)

# Get complete data for first few products to analyze all fields
sample_complete_products = []
sample_variants = []

for i, product in enumerate(sample_products[:3]):  # Analyze first 3 products
    product_url = base_url + '/products/' + product['handle']
    complete_product = get_complete_product_data(product_url)
    if complete_product:
        sample_complete_products.append(complete_product)
        if 'variants' in complete_product and complete_product['variants']:
            sample_variants.extend(complete_product['variants'])

# Determine all available fields
product_fields = get_all_product_fields(sample_complete_products)
variant_fields = get_all_variant_fields(sample_variants) if with_variants else []

if args.debug:
    print(f"\n[DEBUG] Available product fields ({len(product_fields)}):")
    for field in product_fields:
        print(f"  - {field}")
    
    if with_variants and variant_fields:
        print(f"\n[DEBUG] Available variant fields ({len(variant_fields)}):")
        for field in variant_fields:
            print(f"  - {field}")
    print()

# Create CSV with all possible fields
with open('products_complete.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    
    # Create header
    header = ['scraped_url', 'meta_title', 'meta_description']
    header.extend(product_fields)
    
    if with_variants:
        header.extend([f"variant_{field}" for field in variant_fields])
    
    writer.writerow(header)
    
    print(f"[+] Created CSV with {len(header)} columns")
    print("[+] Starting complete data extraction...")
    
    page = 1
    total_products = 0
    total_variants = 0
    
    products = get_page(page)
    while products:
        for product in products:
            product_url = base_url + '/products/' + product['handle']
            print(f" ├ Scraping: {product_url}")
            
            # Get meta tags
            title, description = get_tags_from_product(product_url)
            
            # Get complete product data
            complete_product = get_complete_product_data(product_url)
            if not complete_product:
                continue
            
            # Flatten product data
            flattened_product = flatten_dict(complete_product)
            
            if with_variants and 'variants' in complete_product:
                # Write one row per variant
                for variant in complete_product['variants']:
                    row = [product_url, title, description]
                    
                    # Add all product fields
                    for field in product_fields:
                        row.append(flattened_product.get(field, ''))
                    
                    # Add all variant fields
                    flattened_variant = flatten_dict(variant)
                    for field in variant_fields:
                        row.append(flattened_variant.get(field, ''))
                    
                    writer.writerow(row)
                    total_variants += 1
            else:
                # Write one row per product
                row = [product_url, title, description]
                
                # Add all product fields
                for field in product_fields:
                    row.append(flattened_product.get(field, ''))
                
                writer.writerow(row)
            
            total_products += 1
        
        page += 1
        products = get_page(page)
    
    print(f"\n[+] Scraping completed!")
    print(f"[+] Total products: {total_products}")
    if with_variants:
        print(f"[+] Total variants: {total_variants}")
    print(f"[+] Data saved to: products_complete.csv")
    print(f"[+] Total columns: {len(header)}")
