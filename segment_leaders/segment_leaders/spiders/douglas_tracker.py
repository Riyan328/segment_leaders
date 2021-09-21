import scrapy
import random, string, json, csv, re
from slugify import slugify
import mysql.connector as mysql
from ..items import SegmentLeadersItem

class SpiderTemplateSpider(scrapy.Spider):
    name = 'douglas-tracker'
    allowed_domains = ['www.douglas.de']
    segment_leader_id = 1130567         # Suppliers ID of the current supplier
    # scrapy crawl douglas-tracker -a operation=insert|update    
    operation = ''
    
    def start_requests(self):
        if self.operation == 'insert':
            category_list = self.get_category_list(self.segment_leader_id)
            for category in category_list:
                yield scrapy.Request(category[1], self.parse_category_list, meta={'segment_category_id': category[0]})
        elif self.operation == 'update':
            product_urls = self.get_product_urls('douglas.de')
            for product in product_urls:
                yield scrapy.Request(product[1], self.parse_product_update, meta={'product_id': product[0]})    
        else:
            exit(f"Wrong argument passed: operation={self.operation}")
        
    # DB Connection
    def db_connection(self):
        try:
            # db = mysql.connect(host='localhost', user='riyan', passwd = 'Riyan@328', db='hhff252')
            db = mysql.connect(host='localhost', user='root', passwd='&%^!In^7vT1$7', db='hhff252')
            return db
        except mysql.Error as e:
            print(e)

    # Get Category List
    def get_category_list(self, segment_leader_id):
        connection = self.db_connection()
        cursor = connection.cursor(buffered=True)
        query = f"SELECT id,category_url FROM segment_category_lists WHERE segment_leader_id='{segment_leader_id}' AND trackable=1 and id = 46"
        cursor.execute(query)
        data = cursor.fetchall()
        connection.close()
        return data

    # Get Product URLs
    def get_product_urls(self, url_like):
        connection = self.db_connection()
        cursor = connection.cursor(buffered=True)
        query = f"SELECT id, product_url FROM products WHERE product_url like '%{url_like}%' AND rerun < 1 AND created_at like '2021-06%'"
        cursor.execute(query)
        data = cursor.fetchall()
        connection.close()
        print("Total Count is {}".format(len(data)))
        return data

    def parse_product_update(self, response):
        connection = self.db_connection()
        cur = connection.cursor(buffered=True)
        print("Parsing.......................................")
        pid = response.meta.get('product_id')
        gtin = self.get_ean(response)
        print(gtin, pid)
        cur.execute("update products set gtin = %s where id = %s", (gtin, pid))
        connection.commit()
        connection.close()
        print("######################Product data updated!!\n")

    def parse_category_list(self, response):
        try:
            print("<=====================================Parsing Product Urls=====================================>")
            product = {}
            product['category_id'] = response.meta.get('segment_category_id', '')
            product['category_url'] = response.url
            for res in response.xpath('//div[@class="product-tile product-tile--is-pop-tile"]'):
                if res.xpath("a/div[@class='product-tile__details']/span/text()").get():
                    sponsered = 'Sponsered product'
                    print("This product is Sponsered so left out........................")
                    continue
                product['name'] = res.xpath("a/div[@class='product-tile__details']//div[@class='product-tile__text product-tile__top-brand']/text()").get()
                product_url = res.xpath("a[@class='link link--no-decoration product-tile__main-link']/@href").get()
                try:
                    product['url'] = 'https://www.douglas.de' + product_url.split('?')[0]
                except:
                    product['url'] = 'https://www.douglas.de' + product_url
                product['image'] = res.xpath('a/div[@class="product-tile__image"]//img[@class="image"]').xpath('@srcset|@data-lazy-srcset').get().split('&')[0]
                product['price'] = (res.xpath("a/div[@class='product-tile__details']//div[@class='price-row']/div/text()").get() or res.xpath("a/div[@class='product-tile__details']//div[@class='base-price-row']//span/text()").getall()[4]).strip().replace("\xa0€","").replace(",", ".")
                
                # Product url duplication check
                connection = mysql.connect(host='localhost', user='riyan', passwd = 'Riyan@328', db='hhff252')
                # connection = mysql.connect(host='localhost', user='root', passwd='&%^!In^7vT1$7', db='hhff252')
                cursor = connection.cursor(buffered=True)
                query = f"SELECT id FROM products WHERE product_url = '{product['url']}'"
                cursor.execute(query)
                data = cursor.fetchall()
                connection.close()
                if len(data) == 0:
                    segment_product_flag = 'New'
                    # Send request to product varient
                    yield scrapy.Request(url = product.get('url'), callback=self.parse_product_variant, meta={'segment_category_id': product.get('category_id')})
                else:
                    segment_product_flag = 'Duplicate'
                    print('\n###########Dublicate products found from URL check so left out................')
                
                # Write products details in CSV
                with open('csvs/douglas.csv', 'a') as csvfile:
                    mycsv = csv.writer(csvfile)
                    mycsv.writerow([product.get('name'),product.get('url'), product.get('price'), product.get('image'), segment_product_flag])
                    print('Categories products details saved to local CSV file...................')
 
            # Make recursive call if pagination exists
            next_page_url = response.xpath('//div[@class="pagination-title pagination-title--with-dropdown"]/text()').get()
            for i in range(1, int(next_page_url.split(' ')[3])):
                page_no = i+1
                full_url = response.url
                full_url = full_url.split("?page=")
                url = full_url[0]+'?page='+str(page_no)
                print("\nNEXT URL:\n", url)
                yield response.follow(url, self.parse_category_list, meta={'segment_category_id': product.get('category_id')})
                
        except BaseException as e:
            print("#################################### Exception Occures ############################################")
            filename = 'csvs/Douglas-tracker-Error.csv'
            with open(filename, 'a') as csvfile:
                data = csv.writer(csvfile)
                data.writerow([product.get('category_url'), str(e)])
            print("$"*100)
            print("Tracker execution disturbed due to {}".format(e))
            print("$"*100)
            pass

    def parse_product_variant(self, response):
        print('\n **************************** Parsing Products variants **********************************************************\n')
        product_url = response.url
        segment_cat_id = response.meta.get('segment_category_id', '')
        # Check Variants and Start parsing details
        variants = self.variants_url(response)
        if variants:
            print("\n Variants Found.......................................")
            for i in variants:
                main_url = product_url
                main_url = main_url.split("?variant=")
                variant_url = main_url[0]+'?variant='+str(i)
                print('******************Variants Products url:\n', variant_url)
                yield scrapy.Request(url = variant_url, callback = self.parse_product_detail, dont_filter=True, meta={'segment_category_id': segment_cat_id})
        else:
            print('\n No Variant Present....................................')
            yield scrapy.Request(url = product_url, callback = self.parse_product_detail, dont_filter=True, meta={'segment_category_id': segment_cat_id})


    def parse_product_detail(self, response):
        print('\n ************************************* Parsing Products Details **********************************************************\n')
        items = SegmentLeadersItem()
        segment_category_id = response.meta.get('segment_category_id', '')
        product_id = response.meta.get('product_id', '')
        
        items['product_url'] = response.url
        items['product_name'], items['slug'], items['sku'] = self.pname(response)
        items['product_price'] = self.price(response)
        items['product_summary'] = None
        items['product_description'] = self.description(response)
        items['product_safety'] = None
        items['attributes'] = self.attributes(response)
        items['shipping_weight'] = None
        items['product_dimension'] = None
        items['meta_category'] = self.meta_category(response)
        items['length'] = None
        items['width'] = None
        items['height'] = None
        items['gtin'] = self.get_ean(response)
        items['rerun'] = -3
        items['product_image'] = self.images(response)
        items['product_quantity'] = self.quantity(response)
        items['brand_name'] = self.brand(response)
        items['delivery_date'] = 6
        items['supplier_id'] = self.segment_leader_id
        items['suppliers_list_url'] = response.url
        items['segment_sku'] = self.segment_sku(response)
        items['shipping_cost'] = 3.95
        items['product_type'] = 'sc'
        items['amazon_title'] = None
        items['asin'] =  None
        items['first_available'] = None
        if segment_category_id:
            items['segment_category_id'] = segment_category_id
        items['segment_flag'] = 'New'
        if product_id:
            items['product_id'] = product_id
        items['operation'] = self.operation
        
        # GTIN and segment leader's product sku duplication check
        gtin = items['gtin']
        segment_sku = items['segment_sku']
        if ((gtin and not gtin.isspace()) or (segment_sku and not segment_sku.isspace())):
            # connection = mysql.connect(host='localhost', user='riyan', passwd = 'Riyan@328', db='hhff252')
            connection = mysql.connect(host='localhost', user='root', passwd='&%^!In^7vT1$7', db='hhff252')
            cursor = connection.cursor(buffered=True)
            query = f"SELECT id FROM products WHERE gtin='{gtin}' OR segment_product_sku='{segment_sku}'"
            cursor.execute(query)
            data = cursor.fetchall()
            connection.close()
            if len(data) == 0:
                yield items
            else:
                print('\n Product already Exist in the database ................')


    def variants_url(self, response):
        varient = []
        vas = response.xpath('//div[@class="variant-selector__group-one"]/li[@class="variant-selector__item variant-selector__item--selected"]/@value').getall()
        varient += vas 
        var = response.xpath('//div[@class="variant-selector__group-one"]/li[@class="variant-selector__item"]/@value').getall()
        varient += var
        vl = response.xpath('//li[@class="variant-selector__item variant-selector__item--discount"]/@value').getall()
        varient += vl
        return varient

    def pname(self, response):
        p_name = response.xpath("//div[@class='second-line']//text()").getall()
        p_name = " ".join(name.strip() for name in p_name if len(name)>2)
        p_name = p_name.replace("'","").strip()
        title = slugify(p_name)
        sku = ''.join(random.choice(string.ascii_uppercase + string.digits + string.ascii_lowercase ) for _ in range(10))
        slug = title+"-"+sku 
        return (p_name, slug, sku)
    
    def price(self, response):
        price = response.xpath('//div[@class="product-price"]/div[@class="product-price__discount product-price__discount product-price__discount--discount-color"]/text()').get() or response.xpath("//div[@class='product-price']//text()").get()
        print("Product Price first {}".format(price))
        # self.cnn_module.parse_price(price)
        try:
            product_price = price.strip()
            product_price_int = product_price.split("€")
            try:
                product_price_final = product_price_int[0]
                if ',' in product_price_final:
                    product_price_final = product_price_final.replace("\xa0","").strip()
                price = product_price_final.replace(",",".")
            except IndexError:
                price = 0
        except:
            price = 0
        return price

    def quantity(self, response):
        avail = response.xpath("//div[@class='delivery-info']//text()").get()
        try:
            avail = avail.lower()
            qty = 1 if (avail == "auf lager") else 0
        except:
            qty = 0
        return qty

    def description(self, response):
        desc = response.xpath('//div[@class="truncate product-details__description"]//text()').getall()
        desc = "\n".join(sum.strip() for sum in desc if len(sum)>2).strip()
        return desc

    def attributes(self, response):
        attrs = {}
        for attrib in response.xpath('//div[@class="product-detail-info__classifications"]/div'):
            detail = attrib.xpath('span[@class="classification__item classification__item--bold"]/text()').get()
            attrs[detail] =attrib.xpath('span[@class="classification__item"]/text()').get()
        product_quantity = 'product quantity'
        attrs[product_quantity] = response.xpath('//div[@class="product-detail__variant"]/div/div[@class="product-detail__variant-name"]/text()').get()
        attrs = json.dumps(attrs)
        return attrs
    
    def meta_category(self, response):
        cat = response.xpath('//span[@class="breadcrumb__entry"]//text()').getall()[:-1]
        return str(cat)

    def images(self, response):
        images = response.xpath('//div[@class="image-container thumb"]/img[@class="image"]/@data-lazy-src').getall() or response.xpath('//div[@class="main-media__main-image-container"]//img/@src').getall()
        return images

    def brand(self, response):
        try:
            brand = response.xpath('//span[@class="brand-logo__text brand-logo__text--dynamic"]/text()').get() or response.xpath('//span[@class="brand-logo__text brand-logo__text--fixed"]/text()').get('')
        except:
            brand = response.xpath('//h1[@class="headline-bold"]/div/a/@href').get('')
            if brand:
                brand = brand.split('/')[3]
        return brand
    
    def segment_sku(self, response):
        special_id = response.xpath("//div[@class='product-detail-info__classifications']/div[contains(span/text(),'Art-Nr.')]/span[@class='classification__item']/text()").get()
        return special_id

    def get_ean(self, response):
        script = response.xpath('//script[@id="state-body"]/text()').get()
        ean_value = re.search('"ean"\:\"[0-9]*', script, re.I)
        ean_digit = ean_value.group(0).split(':')[1].replace('"', '')
        return ean_digit
