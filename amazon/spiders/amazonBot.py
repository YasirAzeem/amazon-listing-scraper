import scrapy
from bs4 import BeautifulSoup
import re, json
import sys, random
import mysql.connector
import json
import datetime
import urllib.parse
from slugify import slugify



class AmazonbotSpider(scrapy.Spider):
    name = 'amazonBot'
    allowed_domains = ['amazon.com']
    # start_urls = ['https://www.amz.com/search?q=Plastic+Doilies']
    count = 0
    conn = mysql.connector.connect(host='154.38.160.70',
                                    database='sql_usedpick_com',
                                    user='sql_usedpick_com',
                                    password='e5empmmWBjBEr5s6')
    cursor = conn.cursor()
    
    def start_requests(self):
        kws = [x.replace(',','') for x in open('kws.txt','r').read().split('\n') if x]
        for kw in kws[3000:]:
            url =  f"https://www.amazon.com/s?k={urllib.parse.quote_plus(kw)}&ref=nb_sb_noss"    
            yield scrapy.Request(url=url,callback=self.parse,meta={'kw':kw,'count':1})


 


    def upload_entry(self, item, retries=1):    
        while retries:
            try:
                for i in list(item.keys()):
                    if not item.get(i):
                        item[i]=None
                name =  "amazon"
                conn = mysql.connector.connect(host='154.38.160.70',
                                    database='sql_usedpick_com',
                                    user='sql_usedpick_com',
                                    password='e5empmmWBjBEr5s6')
                conn.autocommit = False
                cursor = conn.cursor(buffered=True)
                try:
                    item_slug = slugify(item['Brand'])
                except:
                    item_slug = ""
                sql_update_query = f'''SELECT id from sql_usedpick_com.brand_list where name = "{item['Brand']}"'''
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                if x:
                    brand_id, = x
                    if type(brand_id)==tuple:
                        brand_id, = brand_id
                    # sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.brand_list (name,slug) VALUES ("{item['Brand']}","{item_slug}");'''
                    # cursor.execute(sql_update_query)
                else:
                    sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.brand_list (name, slug) VALUES ("{item['Brand']}","{item_slug}");'''
                    cursor.execute(sql_update_query)
                    brand_id = cursor.lastrowid
                cat = item['categories']
                if cat:
                    all_cats_list = [x.strip() for x in cat.split('>') if x]
                    indx_dict = {}
                    for kn, cat in enumerate(all_cats_list):
                        indx_dict[cat] = kn
                    cats = ['"'+x.strip()+'"' for x in cat.split('>') if x]
                    child = cat.split('>')[-1]
                    sql_update_query = f'''SELECT * from sql_usedpick_com.categories where category_name IN ({",".join(cats)}) AND type = "{name}";'''
                    cursor.execute(sql_update_query)
                    x = cursor.fetchall()
                    
                    cats_dict = {}
                    for c in x:
                        c = list(c)
                        cats_dict[c[1]] = c[0]
                    catid_list = [str(x) for x in list(cats_dict.values())]
                    if len(cat.split('>'))==len(list(cats_dict.keys())):
                        catid_list = ",".join(catid_list)
                    else:
                        cats_to_add = [c for c in all_cats_list if c not in list(cats_dict.keys())]
                        for i,clr in enumerate(cats_to_add):
                            ct_slug = slugify(clr)
                            parent_id = None
                            for k,ac in enumerate(all_cats_list):
                                if clr==ac:
                                    if k==0:
                                        break
                                    parent_id = cats_dict.get(all_cats_list[k-1])
                                    if not parent_id:
                                        parent_id = catid_list[-1]
                                    break
                            if not parent_id:
                                sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.categories (category_name, slug, type, depth, parent_id, status, user_id) VALUES ("{clr}","{ct_slug}","{name}", {indx_dict[clr]}, null, 0, 1);'''
                            else:
                                sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.categories (category_name, slug, type, depth, parent_id, status, user_id) VALUES ("{clr}", "{ct_slug}","{name}", {indx_dict[clr]},{parent_id}, 0, 1);'''
                            
                            cursor.execute(sql_update_query)
                            catid_list.append(cursor.lastrowid)
                            cats_dict[clr] = cursor.lastrowid
                        catid_list = ",".join([str(x) for x in catid_list])
                else:
                    catid_list = None

                sql_update_query = f'''SELECT id from sql_usedpick_com.amazon_products where site_product_id = "{item['product_id']}";'''
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                for i in list(item.keys()):
                    if type(item[i])==list or type(item[i])==dict:
                        item[i] = json.dumps(item[i])
                    elif type(item[i])==tuple:
                        pass
                if x:
                    product_id, = x
                else:
                    sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.amazon_products (product_reff_url, slug, title, price, site_product_id, images_url, shipping, brand_id, site_specification_data, specification, short_description, long_description, rating, review_count, reviews, item_condition) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
                    cursor.execute(sql_update_query,(item['url'].split('?')[0], item['slug'], item['title'],item['price'],item['product_id'], item['Image_URLs'], item['Shipping'], str(brand_id), item['Site Specific Data'], item['Specifications'], item['Short Desc'], item['Long Desc'], item['rating'], item['rating_count'], item['reviews'], item['Condition']))
                    product_id = cursor.lastrowid
                
                kw_slug = slugify(item['keyword'])
                sql_update_query = f'''SELECT id from sql_usedpick_com.all_keywords where keyword = "{item['keyword']}";'''
                
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                if x:
                    kw_id, = x
                    
                    sql_update_query = f'''UPDATE sql_usedpick_com.all_keywords SET updated_from_amazon = %s WHERE id = "%s";'''
                    cursor.execute(sql_update_query,(datetime.datetime.now(),kw_id))
                    sql_update_query = f'''UPDATE sql_usedpick_com.all_keywords SET slug = %s WHERE id = "%s";'''
                    cursor.execute(sql_update_query,(kw_slug,kw_id))
                    
                else:
                    sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.all_keywords (keyword, slug, updated_from_amazon) VALUES ( %s, %s, %s);'''
                    cursor.execute(sql_update_query,(item['keyword'],kw_slug,datetime.datetime.now()))
                    kw_id = cursor.lastrowid
                
                
                
                sql_update_query = f'''SELECT id from sql_usedpick_com.amazon_keyword where keyword_id = "{kw_id}" AND product_id = {product_id};'''
                
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                if x:
                    pass
                else:
                    sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.amazon_keyword (keyword_id, product_id, index_no) VALUES ("{kw_id}", {product_id}, {item["Index"]});'''
                    cursor.execute(sql_update_query)
                if catid_list:
                    all_category_ids = [int(x.strip()) for x in catid_list.split(',') if x]
                else:
                    all_category_ids = []

                # Categories Table Update

                sql_update_query = f'''SELECT id from sql_usedpick_com.amazon_product_categories where product_id = {product_id} AND keyword_id = {kw_id};'''
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                if x:
                    pass
                else:
                    data = []
                    for cid in all_category_ids:
                        data.append((kw_id, product_id, cid),)
                    sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.amazon_product_categories (keyword_id, product_id, category_id) VALUES (%s, %s, %s);'''
                    cursor.executemany(sql_update_query,data)

                filters = json.loads(item['Site Specific Data'])
                if filters.get('item_specs'):
                    specs = filters.get('item_specs')
                    for spec in list(specs.keys()):
                        try:
                            try:
                                sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.attribute_list (name, slug) VALUES (%s, %s);'''
                                cursor.execute(sql_update_query,(spec, slugify(spec)))
                                filter_id = cursor.lastrowid
                            except Exception as e:
                                # print('Spec Loop ',e)
                                sql_update_query = f'''SELECT id FROM sql_usedpick_com.attribute_list WHERE name = "{spec}";'''
                                cursor.execute(sql_update_query)
                                filter_id, = cursor.fetchone()
                            value = specs[spec]
                            if spec == "item_condition":
                                continue
                            sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.amazon_attribute_details (product_id, keyword_id, filter_id, filter_value) VALUES (%s, %s, %s, %s);'''
                            cursor.execute(sql_update_query,(product_id, kw_id, filter_id, value))
                        except:
                            pass


                if item.get('Color'):
                    if type(item['Color'])!=list:
                        colorss = item['Color'].split(',')
                    else:
                        colorss = item['Color']
                    
                    colors = ['"'+x+'"' for x in colorss]
                else:
                    colorss = []

                if colorss:
                    filter_id = 1
                    for color in colorss:
                        sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.amazon_attribute_details (product_id, keyword_id, filter_id, filter_value) VALUES (%s, %s, %s, %s);'''
                        cursor.execute(sql_update_query,(product_id, kw_id, filter_id, color))


                if item.get('Material'):
                    if type(item['Material'])!=list:
                        colorss = item['Material'].split(',')
                    else:
                        colorss = item['Material']
                    
                    colors = ['"'+x+'"' for x in colorss]
                else:
                    colorss = []

                if colorss:
                    filter_id = 2
                    for color in colorss:
                        sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.amazon_attribute_details (product_id, keyword_id, filter_id, filter_value) VALUES (%s, %s, %s, %s);'''
                        cursor.execute(sql_update_query,(product_id, kw_id, filter_id, color))
                
                conn.commit()
                            
            except mysql.connector.Error as error:
                print(error)    
                retries -=1
                # reverting changes because of exception
                conn.rollback()
                return
            
            except Exception as e:
                print(e)

            finally:
                # closing database connection.
                if conn.is_connected():
                    cursor.close()
                    conn.close()
                    
                return





    def get_num(self,line):
        return re.findall(r"[-+]?(?:\d*\.\d+|\d+)",line)


    def parse(self, response):
        
        soup = BeautifulSoup(response.body,'lxml')
        listings = soup.find_all('div', {'data-component-type':'s-search-result'})
        for inx, li in enumerate(listings):
            item = {}
            item['keyword'] = response.meta['kw']
            item['base'] = response.request.url
            item['Index'] = int(response.meta['count']) + inx
            try:
                item['title'] = li.find('h2').text.strip()
            except:
                try:
                    item['title'] = li.find('a',{'class':'a-text-normal'}).text.strip()
                except:
                    item['title'] = None
                    
                    continue
                    
            item['product_id'] = li.get('data-asin')
            item['url'] =  "https://www.amazon.com"+li.find('a',{'class':'a-link-normal'}).get('href')
            item['slug'] = li.find('a',{'class':'a-link-normal'}).get('href').split('/')[1]
            item['price'] = li.find('span',{'class':'a-price'})
            if item['price']:
                item['price'] = self.get_num(item['price'].find('span',{'class':'a-offscreen'}).text.replace(',',''))
                if item['price']:
                    item['price'] = item['price'][0]
                else:
                    item['price'] = None
            item['rating'] = li.find('span',{'class':'a-icon-alt'})
            if item['rating']:
                item['rating'] = self.get_num(item['rating'].text)
                if item['rating']:
                    item['rating'] = item['rating'][0]
            item['rating_count'] = li.find('span',{'class':"a-size-base s-underline-text"})
            if item['rating_count']:
                item['rating_count']  = abs(int(self.get_num(item['rating_count'].text.replace(',','').strip())[0]))
            item['seller'] = None
            
            cj_num = random.randint(1,99999999)
            # yield item
            if "/bestsellers/" in item['url']:
                return
            yield scrapy.Request(url=item['url'], callback=self.parse2, meta={'item':item,'cookiejar':cj_num})
                                                
        pagination = soup.find('a',{'class':'s-pagination-next'})
        if pagination:
            url = "https://www.amazon.com"+pagination.get('href')
            if url:
                if int(response.meta['count'])+len(listings)<300:
                    yield scrapy.Request(url=url, callback=self.parse,meta={'kw':response.meta['kw'],'count':int(response.meta['count'])+len(listings)})
                else:
                    return

    
    def parse2(self, response):
        s = re.sub('<br\s*?>', '\n', str(response.body))
        soup = BeautifulSoup(s.replace('\\n','\n'),'lxml')
        item = response.meta['item']
        if not item['title']:
            item['title'] = soup.find('h1').text.strip()
        isAvailable = False
        oos = soup.find(id='availability')
        if "in stock" in str(oos).lower():
            isAvailable = True
        emoji_pattern = re.compile(
        u"(\ud83d[\ude00-\ude4f])|"  # emoticons
        u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
        u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
        u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
        u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
        "+", flags=re.UNICODE)

        if emoji_pattern.search(item['title']):
            item['title'] = item['title'].encode('unicode-escape')
        
        if not item['price']:
            item['price'] = soup.find('span',{'class':'a-price'})
            if item['price']:
                item['price'] = self.get_num(item['price'].find('span',{'class':'a-offscreen'}).text.replace(',',''))
                if item['price']:
                    item['price'] = item['price'][0]
                else:
                    item['price'] = None
        try:
            images = [x.text for x in soup.find_all('script',{'type':'text/javascript'}) if 'colorImages' in x.text][0].split("initial")[1].split('}]}')[0]+"}]"

            images = "[{" + images.replace(': [{','').replace("\'",'')
            images = json.loads(images[0:2]+images[3:])

            item['Image_URLs'] = ",".join([x['hiRes'] for x in images if x.get('hiRes') ])
        except:
            item['Image_URLs'] = None
        

        if not item['Image_URLs']:
            image = soup.find(id='imageBlockContainer')
            if image:
                image_url = image.find('img')
                if image_url:
                    image_url = image_url.get('src')
                    if image_url:
                        item['Image_URLs'] = image_url


        if not item['Image_URLs']:
            image = soup.find('img',{'data-a-image-name':'landingImage'})
            if image:
                image_url = image.get('src')
                if image_url:
                    item['Image_URLs'] = image_url

        shipping_cost = -1
        shipping = soup.find(id='mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE')
        if shipping:
            spans = shipping.find_all('span')[0]
            fee = spans.get('data-csa-c-delivery-price')
            if fee:
                if fee.lower()=="free":
                    shipping_cost = 0
                else:
                    shipping_cost = self.get_num(fee)
                    if shipping_cost:
                        shipping_cost = shipping_cost[0]
        if not shipping_cost:
            shipping_cost = -1
        item['Shipping'] = shipping_cost
        brand = soup.find('tr',{'class':'po-brand'})
        if brand:
            brand = brand.find_all('span')[1].text.strip()
        item['Brand'] = brand
        material = soup.find('tr',{'class':'po-material'})
        if material:
            material = material.find_all('span')[1].text.strip()

        color = soup.find('tr',{'class':'po-color'})
        if color:
            color = color.find_all('span')[1].text.strip()
        item['Material'] = material

        description = soup.find(id="productDescription")
        if description:
            description = description.text.strip().strip()

        item['Long Desc'] = description
        item['Short Desc'] = ""
        short_desc = soup.find(id='feature-bullets')
        if short_desc:
            item['Short Desc'] = short_desc.text.strip()    
        
        categories_a = soup.find(id='wayfinding-breadcrumbs_feature_div')
        item['categories'] = None
        if categories_a:
            item['categories'] = ">".join([x.text.strip() for x in soup.find_all('a',{'class':'a-link-normal a-color-tertiary'})])
        else:
            if soup.find_all('span',{'class':'nav-a-content'}):
                # item['categories'] = [x.text.strip() for x in soup.find_all('span',{'class':'nav-a-content'})][0]
                item['categories'] = ""
        if item['categories']:
            if "back to results" in item['categories'].lower():
                if soup.find_all('span',{'class':'nav-a-content'}):
                    item['categories'] = [x.text.strip() for x in soup.find_all('span',{'class':'nav-a-content'})][0]




        weight = soup.find('tr',{'class':'po-item_weight'})
        if weight:
            weight = weight.find_all('span')[1].text.strip()
        
        
        item['Color'] = color
        cond = soup.find('span',{'data-action':'show-all-offers-display'})
        if cond:
            if "New" in cond.text:
                cond = "New"
            elif "Used"  in cond.text:
                cond = "Used"
            else:
                cond = None
        item['Condition'] = cond
        


        length = None
        width = None
        height = None
        
        other_item_specs = soup.find(id='productDetails_detailBullets_sections1')
        item_specs = {}
        item_specs['Dimensions'] = None
        item_specs['Manufacturer'] = None
        if other_item_specs:
            rows = other_item_specs.find_all('tr')
            for row in rows:
                if "Brand" in row.find('th').text and not brand:
                    item['Brand'] = row.find('td').text.strip()
                if "Color" in row.find('th').text and not color:
                    item['Color'] = row.find('td').text.strip()
                if "Material" in row.find('th').text and not material:
                    item['Material'] = row.find('td').text.strip()
                if "Weight" in row.find('th').text and not weight:
                    weight = row.find('td').text.strip()
                if "Dimensions" in row.find('th').text:
                    item_specs['Dimensions'] = row.find('td').text.strip()
                if "Manufacturer" in row.find('th').text:
                    item_specs['Manufacturer'] = row.find('td').text.strip()
                if "Best Sellers Rank" in row.find('th').text and not item['categories']:
                    item['categories'] = ">".join([x.text.strip().split('100 in ')[-1] for x in row.find('td').find_all('a')]) 

        variations = {}
        colors = soup.find(id='variation_color_name')
        if colors:
            colors = colors.find_all('li')
            if colors:
                variations['colors'] = []
                for c in colors:
                    c_dict =  {}
                    c_dict['ASIN'] = c.get('data-defaultasin')
                    c_dict['value'] = c.get('title').strip().split('Click to select ')[-1]
                    variations['colors'].append(c_dict)

            else:
                variations['colors'] = []
        else:
            variations['colors'] = []





        sizes = soup.find(id='variation_size_name')
        if sizes:
            sizes = sizes.find_all('option')
            if sizes:
                variations['sizes'] = []
                for c in sizes:
                    c_dict =  {}
                    c_dict['ASIN'] = c.get('value').split(',')[-1]
                    c_dict['value'] = c.text.strip()
                    variations['colors'].append(c_dict)

            else:
                variations['colors'] = []
        else:
            variations['colors'] = []



        more_specs = {}
        msc = soup.find_all('div',{'class':'product-facts-detail'})
        if msc:
            for m in msc:
                try:
                    more_specs[m.find_all('span')[0].text.strip()] = m.find_all('span')[1].text.strip()
                except:
                    pass

        
        if not more_specs:
            msc = soup.find(id='productDetails_techSpec_section_1')
            if msc:
                msc = msc.find_all('tr')
                for m in msc:
                    try:
                        more_specs[m.find('th').text.strip()] = m.find('td')[1].text.strip()
                    except:
                        pass



        if not more_specs:
            msc = soup.find(id='productOverview_feature_div')
            if msc:
                msc = msc.find_all('tr')
                for m in msc:
                    try:
                        more_specs[m.find_all('span')[0].text.strip()] = m.find_all('span')[1].text.strip()
                    except:
                        pass

        
        

        item_specs['item_specs'] = more_specs
        item_specs['variations'] = variations
        item['Specifications'] = json.dumps({"Weight":weight,"Length":length, "Width":width, "Height": height})
        item['Site Specific Data'] = json.dumps(item_specs)
        item['reviews'] = []

        reviews = soup.find('div',{'class':'reviews-content'})
        if reviews:
            reviews = reviews.find_all('div',{'data-hook':'review'})
            for rev in reviews:
                rev_dict = {}
                name = rev.find('span',{'class':'a-profile-name'})
                if name:
                    rev_dict['author'] = name.text.strip()
                else:
                    rev_dict['author'] = "Not Given"

                msg = rev.find('div',{'class':'reviewText'})
                if msg:
                    rev_dict['content'] = msg.text.strip()
                    if emoji_pattern.search(rev_dict['content']):
                        rev_dict['content'] = rev_dict['content'].encode('unicode-escape')
                else:
                    rev_dict['content'] = None
                rating = rev.find('i',{'data-hook':'review-star-rating'})
                if rating:
                    rating = rating.text.split()[0]
                rev_dict['rating'] = rating
                datee = rev.find('span',{'data-hook':'review-date'})
                if datee:
                    datee = datee.text.split(' on ')[-1]
                rev_dict['date'] = datee
                item['reviews'].append(rev_dict)
        self.upload_entry(item)
        yield item
        
