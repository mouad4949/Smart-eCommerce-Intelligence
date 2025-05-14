import requests
import json
import csv
import time
import logging
import os
from urllib.parse import urlparse, urlunparse

# --- Configuration ---
# Add the base domains of the Shopify stores here
STORE_DOMAINS = [
    "allbirds.com",
    # "gymshark.com",
    # "fashionnova.com",
    # "kyliecosmetics.com",
    # "rothys.com",
    # "tentree.com",
    # "bombas.com",
    # "uk.huel.com",
    # "stevemadden.com",
    # "silkandwillow.com",

    # Add more domains as needed, e.g., "fashionnova.com", "kyliecosmetics.com"
    # Make sure they actually use the /products.json endpoint
]

# Output CSV file name
CSV_FILENAME = "products_data.csv"

# Fields to extract for the CSV file
# Adjust these based on the exact data points you need
CSV_HEADERS = [
    'store_domain',
    'product_id',
    'title',
    'handle',
    'vendor',
    'product_type',
    'created_at',
    'updated_at',
    'published_at',
    'tags', # Will be joined into a single string
    'body_html', # Product description
    'variant_id',
    'variant_title',
    'sku',
    'price',
    'compare_at_price',
    'available',
    'variant_created_at',
    'variant_updated_at',
    'image_src', # URL of the first image
    'all_image_srcs' # All image URLs joined by '|'
]

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions ---
def construct_url(domain):
    """Constructs the full https://domain/products.json URL."""
    parsed = urlparse(f"//{domain}") # Use // to allow urlparse to work
    if not parsed.scheme:
        scheme = "https" # Default to https
    else:
        scheme = parsed.scheme

    # Ensure we have just the domain name (netloc)
    netloc = parsed.netloc or parsed.path # Handle cases where domain is passed without scheme
    if not netloc:
        logging.error(f"Could not parse domain: {domain}")
        return None

    # Reconstruct cleanly
    url = urlunparse((scheme, netloc, "/products.json", "", "", ""))
    return url

def fetch_products(url):
    """Fetches products from a single store's /products.json endpoint."""
    products = []
    page = 1
    limit = 250 # Max limit for Shopify's /products.json
    while True:
        paginated_url = f"{url}?limit={limit}&page={page}"
        logging.info(f"Fetching: {paginated_url}")
        try:
            # Add headers to mimic a browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(paginated_url, timeout=30, headers=headers) # Increased timeout
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

            data = response.json()

            if "products" in data and data["products"]:
                products.extend(data["products"])
                logging.info(f"Fetched {len(data['products'])} products from page {page}. Total so far: {len(products)}")
                # If fewer products than the limit are returned, it's the last page
                if len(data["products"]) < limit:
                    break
                page += 1
            else:
                logging.info(f"No more products found on page {page} or invalid JSON structure for {url}.")
                break # No products key or empty list

            # --- Be polite and avoid rate limiting ---
            time.sleep(1.5) # Wait 1.5 seconds between page requests

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {paginated_url}: {e}")
            break # Stop trying for this domain on error
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from {paginated_url}: {e}")
            logging.error(f"Response text: {response.text[:500]}...") # Log part of the response
            break # Stop trying for this domain
        except Exception as e:
            logging.error(f"An unexpected error occurred for {paginated_url}: {e}")
            break
    return products

def flatten_data(all_products_data, store_domain):
    """Flattens the product and variant data for CSV writing."""
    flattened_rows = []
    for product in all_products_data:
        product_id = product.get('id')
        first_image_src = product.get('images', [{}])[0].get('src') if product.get('images') else None
        all_image_srcs = '|'.join([img.get('src', '') for img in product.get('images', []) if img.get('src')])

        if not product.get('variants'):
            # Handle products with no variants (though rare for standard Shopify setups)
             row = {
                'store_domain': store_domain,
                'product_id': product_id,
                'title': product.get('title'),
                'handle': product.get('handle'),
                'vendor': product.get('vendor'),
                'product_type': product.get('product_type'),
                'created_at': product.get('created_at'),
                'updated_at': product.get('updated_at'),
                'published_at': product.get('published_at'),
                'tags': ', '.join(product.get('tags', [])),
                'body_html': product.get('body_html'),
                'variant_id': None,
                'variant_title': None,
                'sku': None,
                'price': None,
                'compare_at_price': None,
                'available': None,
                'variant_created_at': None,
                'variant_updated_at': None,
                'image_src': first_image_src,
                'all_image_srcs': all_image_srcs,
            }
             flattened_rows.append(row)
        else:
            # Create a row for each variant
            for variant in product.get('variants', []):
                row = {
                    'store_domain': store_domain,
                    'product_id': product_id,
                    'title': product.get('title'),
                    'handle': product.get('handle'),
                    'vendor': product.get('vendor'),
                    'product_type': product.get('product_type'),
                    'created_at': product.get('created_at'), # Product created_at
                    'updated_at': product.get('updated_at'), # Product updated_at
                    'published_at': product.get('published_at'),
                    'tags': ', '.join(product.get('tags', [])),
                    'body_html': product.get('body_html'),
                    'variant_id': variant.get('id'),
                    'variant_title': variant.get('title'),
                    'sku': variant.get('sku'),
                    'price': variant.get('price'),
                    'compare_at_price': variant.get('compare_at_price'),
                    'available': variant.get('available'),
                    'variant_created_at': variant.get('created_at'), # Variant created_at
                    'variant_updated_at': variant.get('updated_at'), # Variant updated_at
                    'image_src': first_image_src, # Use product's first image for simplicity
                    'all_image_srcs': all_image_srcs,
                    # Note: variant.featured_image could be used if needed, but requires mapping
                }
                flattened_rows.append(row)
    return flattened_rows

def save_to_csv(data, filename, headers):
    """Saves the flattened data to a CSV file."""
    file_exists = os.path.isfile(filename)
    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile: # Use 'a' to append
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            if not file_exists or os.path.getsize(filename) == 0:
                 writer.writeheader() # Write header only if file is new or empty
            writer.writerows(data)
        logging.info(f"Successfully appended {len(data)} rows to {filename}")
    except IOError as e:
        logging.error(f"Error writing to CSV file {filename}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during CSV writing: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    all_flattened_data = []

    # Clear the file before starting if you want fresh data each time
    # Or handle appending logic carefully (as done in save_to_csv)
    if os.path.exists(CSV_FILENAME):
         logging.warning(f"{CSV_FILENAME} exists. Appending data. Delete the file manually for a fresh start.")
         # Optional: uncomment to delete the file before running
         # os.remove(CSV_FILENAME)
         # logging.info(f"Removed existing {CSV_FILENAME}.")


    for domain in STORE_DOMAINS:
        logging.info(f"--- Processing domain: {domain} ---")
        url = construct_url(domain)
        if not url:
            continue

        products_data = fetch_products(url)

        if products_data:
            logging.info(f"Fetched a total of {len(products_data)} product entries for {domain}.")
            flattened = flatten_data(products_data, domain)
            if flattened:
                save_to_csv(flattened, CSV_FILENAME, CSV_HEADERS)
            else:
                logging.warning(f"No data flattened for {domain}.")
        else:
            logging.warning(f"No products retrieved for {domain}.")

        # Add a small delay between different domains
        time.sleep(2)

    logging.info("--- Data fetching complete. ---")