import scrapy
from slugify import slugify
from mysql.connector import connect
import random, string, json, csv, re
from ..items import SegmentLeadersItem
from ..commonmodule import CommonModule

class SpiderTemplateSpider(scrapy.Spider):
    name = 'drogerie_markt_tracker'
    segment_leader_id = 1165443         # Suppliers ID of the current supplier
    # scrapy crawl drogerie_markt_tracker -a operation=insert|update    
    operation = ''
    common_module = CommonModule()

    #Connect to the database
    # db = connect(host='localhost', user='root', passwd='&%^!In^7vT1$7', db='hhff252')
    db = connect(host='localhost', user='riyan', passwd = 'Riyan@328', db='hhff252')
    cur = db.cursor(buffered=True)

    def start_requests(self):
        if self.operation == 'insert':
            cat_info = {'110000': 1}
            for k, v in cat_info.items():
                cat_id = k
                for page_no in range(1, v+1):
                    url = f"https://products.dm.de/product/de/search?productQuery=%3Anew%3AallCategories%3A{cat_id}&currentPage={page_no}&purchasableOnly=true&hideFacets=false&hideSorts=false&pageSize=100"
                    # self.execute(f"SELECT id,category_url FROM segment_category_lists WHERE segment_leader_id='{self.segment_leader_id}' AND trackable=1 and remarks = '{cat_id}'")
                    # data_item = self.cur.fetchone()
                    yield scrapy.Request(url, self.parse_category_list, meta={'cat': str(k)})

        elif self.operation == 'update':
            product_urls = self.common_module.get_product_urls('dm.de')
            for product in product_urls:
                yield scrapy.Request(product[1], self.parse_product_detail, meta={'product_id': product[0]})
        else:
            exit(f"Wrong argument passed: operation={self.operation}")

    def parse_category_list(self, response):
        try:
            print("<=====================================Parsing Product Urls=====================================>")
            product = {}
            # product['category_id'] = response.meta.get('segment_category_id', ' ')
            # product['category_url'] = response.meta.get('segment_category_url', ' ')
            product['cat'] = response.meta.get('cat')
            all_data = json.loads(response.text)["products"]
            for data in all_data:
                product['product_url'] = "https://www.dm.de"+data["links"][-1]["href"]
                product['gtin'] = data["gtin"] if ("gtin" in data.keys()) else None
                if not product['gtin']:
                    continue
            
                product['url'] = f"https://services.dm.de/product/de/products/gtins/{product['gtin']}?view=details"
                print(product['url'])
                if self.common_module.asin_uniqueness(product.get('gtin')):
                    segment_product_flag = 'New'
                    yield scrapy.Request(product['url'], callback=self.parse_product_details, meta={'segment_category_id': product['cat'], 'product_url': product['product_url']})
                    print('You Are Here##########################')
                else:
                    segment_product_flag = 'Duplicate'
                    print('\n###########Dublicate products found from Gtin check so left out................')
                
                # Write products details in CSV
                with open('csvs/Drogerie-markt-new.csv', 'a') as csvfile:
                    mycsv = csv.writer(csvfile)
                    mycsv.writerow([product.get('gtin'),product.get('product_url'), segment_product_flag])
                    print('Categories products details of %s saved to local file...................'%product['gtin'])
                
        except BaseException as e:
            print("#################################### Exception Occures ############################################")
            filename = 'csvs/DM-trcker-Error.csv'
            with open(filename, 'a') as csvfile:
                data = csv.writer(csvfile)
                data.writerow([response.url, str(e)])
            print("$"*100)
            print("Tracker execution disturbed due to {}".format(e))
            print("$"*100)
            pass

    def parse_product_details(self, response):
        segment_category_id = response.meta.get('segment_category_id', ' ')
        product_url = response.meta.get('product_url', ' ')
        product_id = response.meta.get('product_id', ' ')
        print("\n ================================================ PRODUCTS DETAIL PARSING STARTED =================================\n")
        items = SegmentLeadersItem()
        data = json.loads(response.text)[0]
        items['product_name'], items['sku'], items['slug'] = self.named(data)
        items['amazon_title'] = None
        items['asin'] =  None
        items['segment_sku'] = str(data["gtin"] if ("gtin" in data.keys()) else None)
        items['brand_name'] = data["brandName"] if ("brandName" in data.keys()) else None
        items['product_url'] = "https://www.dm.de"+data["links"][-1]["href"]
        items['product_price'] = self.price(data)
        items['product_quantity'] = self.quantity(data)
        items['gtin'] = items['segment_sku']
        items['rerun'] = -3
        items['delivery_date'] = 5
        items['supplier_id'] = self.segment_leader_id
        items['shipping_weight'] = 900
        items['length'] = 20
        items['width'] = 20
        items['height'] = 15
        items['product_dimension'] = self.dimension(data)
        items['attributes'] = self.get_attributes(data)
        try:
            items['product_image'] = self.get_image(data)
        except:
            items['product_image'] = None
        items['product_summary'] = None
        items['product_description'] = self.get_description(data)
        items['product_safety'] = self.get_safety(data)
        items['meta_category'] = str(data["details"]["categoryNames"])
        items['first_available'] = None
        items['suppliers_list_url'] = f"https://services.dm.de/product/de/products/gtins/{items['gtin']}?view=details" if items['gtin'] else None
        items['shipping_cost'] = 4.95
        items['product_type'] = 'sc'
        items['segment_flag'] = 'New'
        items['operation'] = self.operation
        if segment_category_id == '110000':
            items['segment_category_id'] = 375
        elif segment_category_id == '010000':
            items['segment_category_id'] = 376
        else:
            items['segment_category_id'] = None

        if product_id:
            items['product_id'] = product_id

        #Check products uniqueness to load it or not
        if self.common_module.is_gtin_or_segment_sku_unique(items['gtin'], items['segment_sku']):
            yield items
        else:
            print('\nProduct already Exist in the database.................................')


    def named(self, data):
        named = data['title']
        named = named.replace("'", "\\'").replace('"', '\\"').replace("`", "\\`")
        title = slugify(named)
        sku = ''.join(random.choice(string.ascii_uppercase + string.digits + string.ascii_lowercase ) for _ in range(10))
        slug = sku+"-"+title
        return (named, sku, slug)

    def price(self, data):
        product_price = data["price"]
        print("Product Price first {}".format(product_price))
        if product_price == 0:
            product_price = data["priceLocalized"]
            print("Product Price Second is {}".format(product_price))
        try:
            product_price = product_price.strip()
            product_price_org = product_price.split("€")
            try:
                product_price_int = product_price_org[0]
                if ',' in product_price_int:
                    product_price = product_price_int.replace("\xa0","").strip()
                product_price = product_price.replace(",",".")
            except IndexError:
                product_price = 0
        except:
            product_price = 0
        return product_price

    def quantity(self, data):
        avail = data["details"]["itemAvailability"]
        print("Availability >> ", avail)
        try:
            avail = avail.lower()
            qty = 1 if (avail == 'instock') else 0
        except:
            avail = data["notDeliverableToStore"]
            qty = 1 if (avail == False) else 0
        return qty

    def get_attributes(self, data):
        try:
            attrs = data["details"]["filterFeatures"]["filterFeatureGroup"]
            for k, v in attrs.items():
                attrs[k] = v[0]
            attrs['Item Number'] = data["dan"]
            attrs['EAN'] = data["gtin"]
            attr = json.dumps(attrs)
        except:
            attr = None
        
        return attr

    def dimension(self, data):
        product_dimension = 0
        try:
            product_dimension = " ".join((str(data["netQuantityContent"]), data["contentUnit"])) if (data["netQuantityContent"] and data["contentUnit"]) else None
        except:
            pass
        
        return product_dimension

    def get_description(self, data):
        try:
            desc = data["details"]["descriptionText"]
            desc = re.sub(r'\s+', ' ', desc)
        except:
            desc = None
        
        return desc

    def get_safety(self, data):
        try:
            desc = data["details"]["descriptionGroup"]["warningLabelDescription"]["text"]
            desc = re.sub(r'\s+', ' ', desc)
        except:
            desc = None
        
        return desc

    def get_image(self, data):
        all_images = []
        try:
            images = data["details"]["images"]
            for image in images:
                image = image[-1]["href"]
                all_images.append(image)
        except:
            all_images = [im["href"] for im in data["links"] if (im["rel"] == "org")]
        
        return all_images
