# Process all products needing details
python load_products_details.py --all

# Process all products with visible browser
python load_products_details.py --all --visible

# Process all products older than 3 days
python load_products_details.py --all --days 3

# Original behavior (limit of 10, default)
python load_products_details.py

# Custom limit (--all takes precedence if both specified)
python load_products_details.py --limit 50