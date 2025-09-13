from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
import asyncio
import requests
from lxml import html
import re
import time
import json
from urllib.parse import urljoin, urlparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MakingCosmetics Scraper API")

# Global scrape status
scraping_status: Dict[str, Any] = {
    'is_running': False,
    'last_run': None,
    'last_result': None,
    'last_full_result': None,
    'error': None,
}


def run_scrape(limit: int | None = None) -> Dict[str, Any]:
    """Blocking scrape function that runs the MakingCosmetics scraper and
    returns a result payload similar to the old Flask response.

    Args:
        limit: Optional cap on number of product links to process (testing)
    """
    try:
        scraping_status['is_running'] = True
        scraping_status['error'] = None
        start_ts = time.time()

        logger.info("Starting product scraping..." + (f" (limit={limit})" if limit else ""))
        scraper = MakingCosmeticsScraper()
        products = scraper.scrape_all_products(limit=limit)

        # Focus on name, size, and price fields
        formatted_products = []
        for product in products:
            formatted_product = {
                "name": product.get('name', ''),
                "sizes": product.get('sizes', []),
                "price_info": product.get('price_info', ''),
                "prices": product.get('prices', {}),
            }
            formatted_products.append(formatted_product)

        duration = round(time.time() - start_ts, 2)
        result: Dict[str, Any] = {
            "success": True,
            "total_products": len(formatted_products),
            "products": formatted_products,
            "statistics": {
                "products_with_sizes": len([p for p in products if p.get('sizes')]),
                "products_with_prices": len([p for p in products if p.get('price_info') or p.get('prices')]),
            },
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duration_sec": duration,
            "status": "completed",
        }

        scraping_status['last_result'] = {
            'total_products': result['total_products'],
            'scraped_at': result['scraped_at'],
            'status': result['status'],
            'duration_sec': result['duration_sec'],
        }
        scraping_status['last_full_result'] = result
        scraping_status['last_run'] = result['scraped_at']
        logger.info("Scraping completed successfully.")
        return result
    except Exception as e:
        error_msg = f"Scraping failed: {str(e)}"
        logger.error(error_msg)
        scraping_status['error'] = error_msg
        scraping_status['last_result'] = {'status': 'failed'}
        raise
    finally:
        scraping_status['is_running'] = False


@app.get('/health')
def health_check():
    return {
        'status': 'healthy',
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'scraping_status': {
            'is_running': scraping_status['is_running'],
            'last_run': scraping_status['last_run'],
        },
    }


@app.post('/scrape_async')
async def scrape_async(background_tasks: BackgroundTasks, wait: bool = False, timeout: int = 120, limit: int | None = None):
    """Start scraping in the background. Optionally wait for completion.

    Query params:
    - wait: if true, wait up to `timeout` seconds for completion
    - timeout: seconds to wait when wait=true
    - limit: optional cap on number of product links to process (testing)
    """
    if scraping_status['is_running']:
        if wait:
            deadline = time.time() + max(1, timeout)
            while scraping_status['is_running'] and time.time() < deadline:
                await asyncio.sleep(1)
            if not scraping_status['is_running'] and scraping_status.get('last_full_result') is not None:
                return scraping_status['last_full_result']
            return {'status': 'running'}
        return {'status': 'running'}

    # queue background task with limit
    background_tasks.add_task(run_scrape, limit)

    if wait:
        deadline = time.time() + max(1, timeout)
        while scraping_status['is_running'] and time.time() < deadline:
            await asyncio.sleep(1)
        if not scraping_status['is_running'] and scraping_status.get('last_full_result') is not None:
            return scraping_status['last_full_result']
        return {'status': 'running'}

    return {'status': 'accepted'}


@app.get('/status')
def status():
    return {
        'is_running': scraping_status['is_running'],
        'last_run': scraping_status['last_run'],
        'last_result': scraping_status['last_result'],
        'error': scraping_status['error'],
    }

class MakingCosmeticsScraper:
    def __init__(self):
        self.base_url = "https://makingcosmetics.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.products = []
        
    def get_all_product_links(self):
        """Find all product links from the comprehensive Ingredients A-Z list page"""
        product_links = set()
        
        # Use the comprehensive product list page
        list_page_url = "https://makingcosmetics.com/Ingredients-A-Z_ep_1.html?lang=default"
        
        try:
            # logger.info("Fetching comprehensive product list from Ingredients A-Z page...")
            response = self.session.get(list_page_url, timeout=15)
            response.raise_for_status()
            tree = html.fromstring(response.content)
            
            # Find all product links on the comprehensive list page
            all_links = tree.xpath('//a/@href')
            
            for link in all_links:
                if link and self.is_product_url(link):
                    # Normalize URL by removing problematic query parameters
                    normalized_link = link.replace('?lang=default', '')
                    full_url = urljoin(self.base_url, normalized_link)
                    product_links.add(full_url)
                        
            logger.info(f"Found {len(product_links)} product links from comprehensive A-Z list")
            
        except Exception as e:
            logger.error(f"Error fetching comprehensive product list: {str(e)}")
                
        return list(product_links)
    
    def is_product_url(self, url):
        """Check if URL is a product page"""
        if not url:
            return False
            
        # Exclude formula/category/service pages
        exclude_patterns = [
            r'_ep_\d+\.html',
            r'Service-',
            r'Formulas',
            r'/search\?',
            r'consultation',
            r'customization'
        ]
        
        for pattern in exclude_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return False
                
        # Product URLs have specific formats like ABC-DEF-01.html
        product_patterns = [
            r'[A-Z]{3,4}-[A-Z0-9]+-\d+\.html',
            r'/[A-Z]{3,4}-[A-Z0-9]+\-\d+\.html'
        ]
        
        for pattern in product_patterns:
            if re.search(pattern, url):
                return True
                
        return False
    
    def call_product_variation_api(self, variation_url):
        """Call MakingCosmetics Product-Variation API to get price for a specific size"""
        try:
            # Ensure absolute URL and add proper headers for Demandware API
            absolute_url = urljoin(self.base_url, variation_url)
            headers = {
                'Accept': 'application/json, text/html, */*',
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': self.base_url
            }
            response = self.session.get(absolute_url, timeout=10, headers=headers)
            response.raise_for_status()
            
            # The response should contain price information - let's parse it
            if response.text:
                # Parse JSON response if possible
                try:
                    json_data = response.json()
                    # Handle different response structures
                    if 'product' in json_data and 'price' in json_data['product']:
                        price_data = json_data['product']['price']
                        if 'sales' in price_data and 'formatted' in price_data['sales']:
                            return price_data['sales']['formatted']
                except (json.JSONDecodeError, KeyError):
                    pass
                    
                # Fallback to regex patterns
                price_patterns = [
                    r'"price"[^}]*"formatted"\s*:\s*"([^"]+)"',
                    r'"sales"[^}]*"formatted"\s*:\s*"([^"]+)"',
                    r'\$([\d,]+(?:\.\d{2})?)',
                    r'"value"\s*:\s*([\d,]+(?:\.\d{2})?)',
                ]
                
                for pattern in price_patterns:
                    match = re.search(pattern, response.text)
                    if match:
                        price_str = match.group(1)
                        if '$' not in price_str:
                            try:
                                price_val = float(price_str.replace(',', ''))
                                return f'${price_val:.2f}'
                            except ValueError:
                                continue
                        return price_str
                    
        except requests.exceptions.RequestException as e:
            logger.debug(f"Request error calling variation API {variation_url}: {str(e)}")
        except Exception as e:
            logger.debug(f"Unexpected error calling variation API {variation_url}: {str(e)}")
            
        return None
    
    def extract_variants_from_options(self, tree):
        """Extract size-price variants from select options with dynamic API calls"""
        variants = []
        try:
            # Look for select elements with size/variant options (MakingCosmetics specific)
            options = tree.xpath('//select[contains(@class, "select-Size")]//option | //select[contains(@name, "size")]//option')
            if not options:
                options = tree.xpath('//select[contains(@name, "dwvar_")]//option')
            
            base_price = None
            # Try to find base price for delta calculations
            base_price_elem = tree.xpath('//span[contains(@class, "price")]//text() | //div[contains(@class, "sales")]//text()')
            if base_price_elem:
                for price_text in base_price_elem:
                    price_match = re.search(r'\$([\d,]+\.\d{2})', price_text)
                    if price_match:
                        base_price = float(price_match.group(1).replace(',', ''))
                        break
            
            for option in options:
                option_text = (option.text or '').strip()
                if not option_text or option_text in ['Choose Options', 'Select Option', 'Select Size', '']:
                    continue
                    
                # Extract size from option text - for MakingCosmetics, keep full text like "1.0floz / 30ml"
                size_pattern = r'\b\d+(?:\.\d+)?\s*(?:fl\s*)?(?:oz|g|ml|kg|lb|gram|liter|L)\b'
                size_matches = re.findall(size_pattern, option_text, re.IGNORECASE)
                
                size = None
                if size_matches:
                    # For MakingCosmetics, use the full option text as size
                    size = option_text.strip()
                elif any(unit in option_text.lower() for unit in ['oz', 'ml', 'g', 'kg', 'lb']):
                    size = option_text.strip()
                
                if size:
                    # Check for MakingCosmetics dynamic pricing
                    option_price = None
                    variation_url = option.get('value')
                    
                    # Method 1: Call Product-Variation API for dynamic pricing
                    if variation_url and 'Product-Variation' in variation_url:
                        # logger.debug(f"Calling variation API for size {size}")
                        option_price = self.call_product_variation_api(variation_url)
                        # if option_price:
                        #     logger.debug(f"Got price {option_price} for size {size} via API")
                    
                    # Method 2: Fallback to data attributes  
                    if not option_price:
                        data_price = option.get('data-price')
                        if data_price:
                            price_match = re.search(r'([\d,]+(?:\.\d{2})?)', data_price)
                            if price_match:
                                price_val = float(price_match.group(1).replace(',', ''))
                                option_price = f'${price_val:.2f}'
                    
                    # Method 3: Price delta in option text (+ $X.XX)
                    if not option_price and base_price:
                        delta_match = re.search(r'\(\+\s*\$([\d,]+\.\d{2})\)', option_text)
                        if delta_match:
                            delta = float(delta_match.group(1).replace(',', ''))
                            option_price = f'${base_price + delta:.2f}'
                        elif '(' not in option_text:  # No delta means base price
                            option_price = f'${base_price:.2f}'
                    
                    # Method 4: Direct price in option text (broader patterns)
                    if not option_price:
                        price_match = re.search(r'\$([\d,]+(?:\.\d{2})?)', option_text)
                        if price_match:
                            price_str = price_match.group(1).replace(',', '')
                            price_val = float(price_str) if '.' in price_str else float(price_str + '.00')
                            option_price = f'${price_val:.2f}'
                    
                    variant = {
                        'size': size,
                        'price': option_price,
                        'source': 'dynamic_api' if (variation_url and 'Product-Variation' in variation_url) else 'option_data'
                    }
                    variants.append(variant)
                    
        except Exception as e:
            logger.debug(f"Error extracting variants from options: {str(e)}")
            
        # Also check radio inputs and UL/LI option lists
        try:
            # Method 1: Radio inputs with labels
            radio_options = tree.xpath('//input[@type="radio" and (contains(@name, "size") or contains(@name, "option"))]')
            for radio in radio_options:
                # Look for following label
                label_elem = radio.xpath('./following-sibling::label[1] | ./parent::label')[0] if radio.xpath('./following-sibling::label[1] | ./parent::label') else None
                if label_elem is not None:
                    label_text = (label_elem.text_content() or '').strip()
                    
                    # Extract size
                    size_pattern = r'\b\d+(?:\.\d+)?\s*(?:fl\s*)?(?:oz|g|ml|kg|lb|gram|liter|L)\b'
                    size_matches = re.findall(size_pattern, label_text, re.IGNORECASE)
                    size = re.sub(r'\s+', ' ', size_matches[0].strip()) if size_matches else None
                    
                    # Extract price
                    price = None
                    data_price = radio.get('data-price') or radio.get('data-price-diff') or radio.get('data-calcprice')
                    if data_price:
                        price_match = re.search(r'([\d,]+(?:\.\d{2})?)', data_price)
                        if price_match:
                            price_val = float(price_match.group(1).replace(',', ''))
                            price = f'${price_val:.2f}'
                    
                    if not price:
                        price_match = re.search(r'\$([\d,]+(?:\.\d{2})?)', label_text)
                        if price_match:
                            price_str = price_match.group(1).replace(',', '')
                            price_val = float(price_str) if '.' in price_str else float(price_str + '.00')
                            price = f'${price_val:.2f}'
                    
                    if size:
                        variant = {
                            'size': size,
                            'price': price,
                            'source': 'radio_option'
                        }
                        variants.append(variant)
            
            # Method 2: UL/LI option lists
            option_lists = tree.xpath('//ul[contains(@class, "option") or contains(@class, "size")] | //div[contains(@class, "option")]/ul')
            for ul in option_lists:
                li_elements = ul.xpath('.//li | .//label')
                for li in li_elements:
                    li_text = (li.text_content() or '').strip()
                    
                    # Extract size
                    size_pattern = r'\b\d+(?:\.\d+)?\s*(?:fl\s*)?(?:oz|g|ml|kg|lb|gram|liter|L)\b'
                    size_matches = re.findall(size_pattern, li_text, re.IGNORECASE)
                    size = re.sub(r'\s+', ' ', size_matches[0].strip()) if size_matches else None
                    
                    # Extract price
                    price = None
                    price_match = re.search(r'\$([\d,]+(?:\.\d{2})?)', li_text)
                    if price_match:
                        price_str = price_match.group(1).replace(',', '')
                        price_val = float(price_str) if '.' in price_str else float(price_str + '.00')
                        price = f'${price_val:.2f}'
                    
                    # Also check for bracketed prices like [+$2.50]
                    if not price:
                        bracket_match = re.search(r'\[\+?\$([\d,]+(?:\.\d{2})?)\]', li_text)
                        if bracket_match:
                            price_str = bracket_match.group(1).replace(',', '')
                            price_val = float(price_str) if '.' in price_str else float(price_str + '.00')
                            price = f'${price_val:.2f}'
                    
                    if size:
                        variant = {
                            'size': size,
                            'price': price,
                            'source': 'ul_option'
                        }
                        variants.append(variant)
                        
        except Exception as e:
            logger.debug(f"Error extracting radio/UL variants: {str(e)}")
            
        return variants
    
    def extract_variants_from_json_ld(self, tree):
        """Extract variants from JSON-LD structured data"""
        variants = []
        try:
            json_scripts = tree.xpath('//script[@type="application/ld+json"]//text()')
            for script_content in json_scripts:
                try:
                    json_data = json.loads(script_content)
                    # Handle both single objects and arrays
                    items = json_data if isinstance(json_data, list) else [json_data]
                    
                    for item in items:
                        if item.get('@type') == 'Product' and 'offers' in item:
                            offers = item['offers']
                            if not isinstance(offers, list):
                                offers = [offers]
                                
                            for offer in offers:
                                # Extract size from description, name, or sku
                                size = None
                                for field in ['description', 'name', 'sku']:
                                    if field in offer:
                                        size_pattern = r'\b\d+(?:\.\d+)?\s*(?:fl\s*)?(?:oz|g|ml|kg|lb|gram|liter|L)\b'
                                        size_matches = re.findall(size_pattern, offer[field], re.IGNORECASE)
                                        if size_matches:
                                            size = re.sub(r'\s+', ' ', size_matches[0].strip())
                                            break
                                
                                price = offer.get('price')
                                if size and price:
                                    variant = {
                                        'size': size,
                                        'price': f'${float(price):.2f}',
                                        'source': 'json_ld'
                                    }
                                    variants.append(variant)
                except json.JSONDecodeError:
                    continue
                    
        except Exception as e:
            logger.debug(f"Error extracting JSON-LD variants: {str(e)}")
            
        return variants
    
    def extract_variants_from_inline_json(self, response_text):
        """Extract variants from inline JavaScript JSON data"""
        variants = []
        try:
            # Look for common patterns of inline product data
            patterns = [
                r'var\s+product\s*=\s*(\{.+?\});',
                r'window\.product\s*=\s*(\{.+?\});',
                r'"variants"\s*:\s*(\[.+?\])',
                r'"options"\s*:\s*(\[.+?\])'  
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, response_text, re.DOTALL | re.IGNORECASE)
                for match in matches:
                    try:
                        json_data = json.loads(match)
                        
                        # Handle variants array
                        if isinstance(json_data, list):
                            variants_data = json_data
                        elif 'variants' in json_data:
                            variants_data = json_data['variants']
                        elif 'options' in json_data:
                            variants_data = json_data['options']
                        else:
                            continue
                            
                        for variant_data in variants_data:
                            size = None
                            price = None
                            
                            # Extract size from various fields
                            for field in ['title', 'name', 'option1', 'option2', 'size']:
                                if field in variant_data:
                                    size_pattern = r'\b\d+(?:\.\d+)?\s*(?:fl\s*)?(?:oz|g|ml|kg|lb|gram|liter|L)\b'
                                    size_matches = re.findall(size_pattern, str(variant_data[field]), re.IGNORECASE)
                                    if size_matches:
                                        size = re.sub(r'\s+', ' ', size_matches[0].strip())
                                        break
                            
                            # Extract price
                            for field in ['price', 'price_min', 'price_max']:
                                if field in variant_data:
                                    price_val = variant_data[field]
                                    if isinstance(price_val, (int, float)):
                                        price = f'${float(price_val):.2f}'
                                        break
                                    elif isinstance(price_val, str):
                                        price_match = re.search(r'([\d,]+\.\d{2})', price_val)
                                        if price_match:
                                            price = f'${float(price_match.group(1).replace(",", "")):.2f}'
                                            break
                            
                            if size and price:
                                variant = {
                                    'size': size,
                                    'price': price,
                                    'source': 'inline_json'
                                }
                                variants.append(variant)
                                
                    except json.JSONDecodeError:
                        continue
                        
        except Exception as e:
            logger.debug(f"Error extracting inline JSON variants: {str(e)}")
            
        return variants
    
    def extract_variants_from_tables(self, tree):
        """Extract size-price pairs from HTML tables"""
        variants = []
        try:
            # Look for tables with Size and Price headers
            tables = tree.xpath('//table')
            for table in tables:
                headers = table.xpath('.//th//text() | .//td[1]//text()')
                header_text = ' '.join(headers).lower()
                
                if 'size' in header_text and 'price' in header_text:
                    rows = table.xpath('.//tr[position()>1]')  # Skip header row
                    for row in rows:
                        cells = row.xpath('.//td//text()')
                        if len(cells) >= 2:
                            size_text = cells[0].strip()
                            price_text = cells[1].strip()
                            
                            # Extract size
                            size_pattern = r'\b\d+(?:\.\d+)?\s*(?:fl\s*)?(?:oz|g|ml|kg|lb|gram|liter|L)\b'
                            size_matches = re.findall(size_pattern, size_text, re.IGNORECASE)
                            size = re.sub(r'\s+', ' ', size_matches[0].strip()) if size_matches else None
                            
                            # Extract price
                            price_match = re.search(r'\$([\d,]+\.\d{2})', price_text)
                            price = f'${float(price_match.group(1).replace(",", "")):.2f}' if price_match else None
                            
                            if size and price:
                                variant = {
                                    'size': size,
                                    'price': price,
                                    'source': 'table'
                                }
                                variants.append(variant)
                                
        except Exception as e:
            logger.debug(f"Error extracting table variants: {str(e)}")
            
        return variants
    
    def extract_variants_from_proximity(self, tree):
        """Extract size-price pairs using DOM proximity as fallback"""
        variants = []
        try:
            # Find all size mentions in text
            all_text_elements = tree.xpath('//*[text()]')
            
            for element in all_text_elements:
                text = element.text or ''
                size_pattern = r'\b\d+(?:\.\d+)?\s*(?:fl\s*)?(?:oz|g|ml|kg|lb|gram|liter|L)\b'
                size_matches = re.findall(size_pattern, text, re.IGNORECASE)
                
                if size_matches:
                    size = re.sub(r'\s+', ' ', size_matches[0].strip())
                    
                    # Look for price in nearby elements (following siblings, parent, etc.)
                    price = None
                    search_elements = (
                        element.xpath('.//following-sibling::*[position()<=3]//*[text()]') +
                        element.xpath('.//parent::*//*[text()]')[:10]
                    )
                    
                    for search_elem in search_elements:
                        search_text = search_elem.text or ''
                        price_match = re.search(r'\$([\d,]+\.\d{2})', search_text)
                        if price_match:
                            price = f'${float(price_match.group(1).replace(",", "")):.2f}'
                            break
                    
                    if size and price:
                        variant = {
                            'size': size,
                            'price': price,
                            'source': 'proximity'
                        }
                        variants.append(variant)
                        break  # Only take first proximity match to avoid duplicates
                        
        except Exception as e:
            logger.debug(f"Error extracting proximity variants: {str(e)}")
            
        return variants
    
    def scrape_product_details(self, product_url):
        """Scrape individual product page for details with enhanced price extraction"""
        try:
            response = self.session.get(product_url, timeout=15)
            response.raise_for_status()
            tree = html.fromstring(response.content)
            
            product = {
                'name': '',
                'sizes': [],
                'prices': {},
                'price_info': '',
                'price_sources': []  # Track which sources provided prices
            }
            
            # Product name
            name_selectors = [
                '//h1[@class="product-name"]//text()',
                '//h1[contains(@class, "product-title")]//text()',
                '//div[contains(@class, "product-name")]//text()',
                '//title//text()'
            ]
            
            for selector in name_selectors:
                names = tree.xpath(selector)
                if names:
                    product['name'] = ' '.join([n.strip() for n in names if n.strip()])
                    break
                    
            # Enhanced Size and Price Extraction with Multiple Sources
            all_variants = []
            
            # Priority 1: Extract variants from select options with data attributes
            variants_from_options = self.extract_variants_from_options(tree)
            all_variants.extend(variants_from_options)
            
            # Priority 2: Extract variants from JSON-LD structured data
            if not all_variants:
                variants_from_json_ld = self.extract_variants_from_json_ld(tree)
                all_variants.extend(variants_from_json_ld)
            
            # Priority 3: Extract variants from inline JavaScript JSON
            if not all_variants:
                variants_from_inline_json = self.extract_variants_from_inline_json(response.text)
                all_variants.extend(variants_from_inline_json)
            
            # Priority 4: Extract variants from HTML tables
            if not all_variants:
                variants_from_tables = self.extract_variants_from_tables(tree)
                all_variants.extend(variants_from_tables)
            
            # Priority 5: Extract variants using DOM proximity (fallback)
            if not all_variants:
                variants_from_proximity = self.extract_variants_from_proximity(tree)
                all_variants.extend(variants_from_proximity)
            
            # Process variants into product data
            sources_used = set()
            for variant in all_variants:
                if variant['size'] and variant['size'] not in product['sizes']:
                    product['sizes'].append(variant['size'])
                    
                if variant['price']:
                    product['prices'][variant['size']] = variant['price']
                    sources_used.add(variant['source'])
            
            product['price_sources'] = list(sources_used)
            
            # Create price_info summary
            if product['prices']:
                price_values = list(set(product['prices'].values()))
                product['price_info'] = ' | '.join(price_values)
            
            # Fallback: Extract sizes from text content if no variants found
            if not product['sizes']:
                all_text_content = ' '.join(tree.xpath('//text()'))
                size_patterns = [
                    r'\b\d+\.?\d*\s*(oz|ounce|ounces)\b',
                    r'\b\d+\.?\d*\s*(g|gram|grams)\b', 
                    r'\b\d+\.?\d*\s*(ml|milliliter|milliliters)\b',
                    r'\b\d+\.?\d*\s*(kg|kilogram|kilograms)\b',
                    r'\b\d+\.?\d*\s*(lb|pound|pounds)\b',
                    r'\b\d+\.?\d*\s*(fl\s*oz|fluid\s*ounce)\b'
                ]
                
                for pattern in size_patterns:
                    for match in re.finditer(pattern, all_text_content, re.IGNORECASE):
                        size_text = match.group(0)
                        if size_text and size_text not in product['sizes']:
                            product['sizes'].append(size_text.strip())
            
            sources_info = f" | Sources: {', '.join(product['price_sources'])}" if product['price_sources'] else ""
            # logger.info(f"Scraped product: {product['name']} | Sizes: {len(product['sizes'])} | Prices: {len(product.get('prices', {}))}{sources_info}")
            return product
            
        except Exception as e:
            logger.error(f"Error scraping product {product_url}: {str(e)}")
            return None
            
    def scrape_all_products(self, limit: int | None = None):
        """Main method to scrape all products

        Args:
            limit: Optional cap on number of product links to process (testing)
        """
        # logger.info("Starting to scrape all products...")
        
        # Get all product links
        product_links = self.get_all_product_links()
        # logger.info(f"Found {len(product_links)} total product links")

        # Optional limit for testing
        if limit is not None and isinstance(limit, int) and limit > 0:
            product_links = product_links[:limit]
            logger.info(f"Limiting to first {len(product_links)} product links (testing)")
        
        # Scrape each product
        scraped_count = 0
        for i, product_url in enumerate(product_links):
            try:
                product = self.scrape_product_details(product_url)
                if product and product.get('name'):
                    self.products.append(product)
                    scraped_count += 1
                    
                # Be respectful with requests
                time.sleep(1)
                
                # if (i + 1) % 10 == 0:
                #     logger.info(f"Scraped {scraped_count} products so far...")
                    
            except Exception as e:
                logger.error(f"Error processing {product_url}: {str(e)}")
                continue
                
        # logger.info(f"Finished scraping! Total products: {len(self.products)}")
        
        # Log statistics
        products_with_sizes = len([p for p in self.products if p.get('sizes')])
        products_with_prices = len([p for p in self.products if p.get('price_info') or p.get('prices')])
        # logger.info(f"Products with sizes: {products_with_sizes}/{len(self.products)}")
        # logger.info(f"Products with prices: {products_with_prices}/{len(self.products)}")
        
        return self.products

@app.api_route('/scrape', methods=['GET', 'POST'])
def scrape_sync(limit: int | None = None):
    if scraping_status['is_running']:
        raise HTTPException(status_code=409, detail='Scraping is already in progress')
    result = run_scrape(limit=limit)
    return result

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=False)