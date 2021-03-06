#!/usr/bin/env python
#-*- coding:utf-8 -*-

import requests
from lxml import etree
import time
import re
import logging
import sys
import pymysql
import json
from datetime import datetime
from optparse import OptionParser

logger = logging.getLogger('SCRAPING-1STDLIBS')
fmt = '[%(name)s][%(levelname)s]:%(message)s'
h = logging.StreamHandler(sys.stdout)
h.setFormatter(logging.Formatter(fmt))
logger.addHandler(h)
logger.setLevel(logging.WARN)

def is_new_product(conn, cursor, product_id):
    sql = 'select * from product where product_id=%d' % product_id
    cursor.execute(sql)
    product = cursor.fetchone()

    if product:
        return False
    return True

def get_page(conn, cursor, session, link, category_id, category2_id, category3_id):

    base_url = 'https://www.1stdibs.com'
    html = session.get(link)
    sel = etree.HTML(html.text)
    items = sel.xpath('//div[@class="product-container"]')
    for item in items:
        product_link = item.xpath('./a/@href')
        product_id = 0
        if not product_link:
            continue
        product_link = product_link[0]
        product_id = int(product_link[product_link.find('id-f_') + 5: -1])
        product_link = base_url + product_link
        product_image = item.xpath('./a/div/img/@src')
        product_image = product_image[0] if product_image else ''
        if product_image.endswith('.gif'):
            product_image = item.xpath('./a/div/noscript/img/@src')[0]
        if is_new_product(conn, cursor, product_id):
            logger.info('scraping product:[%s] ...', product_link)
            loop_enter = time.time()
            product = get_url(conn, cursor, session, product_link, category_id, category2_id, category3_id)
            loop_leave = time.time()
            logger.info('scraping product:[%s] cost %2f sec ...', product_link, loop_leave - loop_enter)
            #add exception handler
            if not product:
                logger.error('parse product(%s) failed this time, try next time ...', product_id)
                continue
            if put_product(conn, cursor, product):
                put_status(conn, cursor, product_id, product['price'], product['status'], product_link, product_image,
                       category_id, category2_id, category3_id)
            else:
                logger.error('trace:%s', product_link)
        else:
            product_price = item.xpath('./span[contains(@class, "product-price")]/span/@data-usd')
            product_status = 0
            if product_price:
                product_price = product_price[0]
                product_price = [x for x in product_price if x.isdigit()]
                product_price = int(''.join(product_price))
            else:
                product_price = 0
                if item.xpath('./span[contains(@class, "product-price")]/span/@data-hide-price'):
                    #hide price, consider on sale
                    pass
                else:
                    product_status = item.xpath('./span[contains(@class, "product-price")]/span/@data-hold')
                    if product_status:
                        product_status = 1
                    else:
                        product_status = 2
            last_status = get_last_status(conn, cursor, product_id)
            if last_status != product_status:
                put_status(conn, cursor, product_id, product_price, product_status, product_link, product_image,
                           category_id, category2_id, category3_id)

            if (product_price > 0):
                update_product(conn, cursor, product_id, product_price)

def get_url(conn, cursor, session, link, category_id, category2_id, category3_id):

    item = {}

    html = session.get(link)

    data = re.search('window.__SERVER_VARS__.data = (.*?\\});', html.text, re.S)
    if not data:
        return item
    data = data.group(1)
    data = json.loads(data)

    detailLinks = re.search('window.__SERVER_VARS__.detailLinks = (.*?\\});', html.text, re.S)
    
    detailLinks = detailLinks.group(1)
    try:
        detailLinks = json.loads(detailLinks)
    except Exception as e:
        detailLinks = {}

    returnscopy = re.search('window.__SERVER_VARS__.returnsCopy = (.*?\\});', html.text, re.S)
    returnscopy = returnscopy.group(1)
    returnscopy = json.loads(returnscopy)
    #dealer
    carousel = re.search('window.__SERVER_VARS__.carousel = (.*?\\});', html.text, re.S)

    carousel = carousel.group(1)
    carousel = json.loads(carousel)
    dealer = ''
    dealer_location = ''
    dealer_link = ''
    for x in carousel['items']:
        if x.get('seller', None):
            dealer = x['seller']['company']
            dealer_location = x['seller']['address']
            dealer_link = 'https://www.1stdibs.com' + x['seller']['uri']
            break

    item['dealer'] = dealer
    item['dealer_location'] = dealer_location
    item['dealer_link'] = dealer_link

    data_item = re.search('window.__SERVER_VARS__.item = (.*?\\});', html.text, re.S)
    data_item = data_item.group(1)
    data_item = json.loads(data_item)

    product_id = data['id']
    title = data['titleCondensed'].replace('"','')
    title = title.replace("'",'')
    title = title.replace("\\",'')
    price = data['retailPrice']['USD'] if data['retailPrice'] else 0
    status = 0
    if data['isSold']:
        status = 2
    elif data['isHold']:
        status = 1

    item['product_id'] = int(product_id)
    item['title'] = title
    item['price'] = int(price)
    item['status'] = status

    period_of = detailLinks.get('periodOf', {})
    period_of = ', '.join(period_of.keys())

    origin = detailLinks.get('placeOfOrigin', {})
    origin = ', '.join(origin)

    period = detailLinks.get('period', {})
    period = ', '.join(period)
    period = period.replace('"', '')
    period = period.replace("'", '')

    creator = detailLinks.get('creator', {})
    creator = ', '.join(creator)
    creator = creator.replace('"', '')
    creator = creator.replace("'", '')

    material = detailLinks.get('materialsAndTechniques', {})
    material = ', '.join(material)

    style_of = detailLinks.get('styleOf', {})
    style_of = ', '.join(style_of)

    #new field
    shipping = data.get('shippingCopy', {}).get('pdpShippingDescriptionNoQuotes', {}).get('value', '')
    number_of_items = data_item.get('num_item', 0)
    condition = data_item.get('condition', '')
    if condition:
        condition = condition.replace('"', '')
    wear = data_item.get('wear', '')
    date_of_manufacture = data_item.get('creation_date', '')
    if not date_of_manufacture:
        date_of_manufacture = ''
    else:
        date_of_manufacture = date_of_manufacture.replace('"', '')
    dimension = data_item.get('measurements', {})
    dimension = 'height:%s, width:%s, depth:%s' % \
                (dimension.get('height', ''), dimension.get('width', ''), dimension.get('depth', ''))
    
    return_policy = returnscopy.get('cannotBeReturned', '')
    seller_since = data_item.get('dealer', {}).get('since_year', '')
    typical_response_time = data.get('sellerResponseTime', '')
    
    item['shipping'] = shipping
    item['number_of_items'] = number_of_items
    item['condition'] = condition
    item['wear'] = wear
    item['date_of_manufacture'] = date_of_manufacture
    item['dimension'] = dimension
    item['return_policy'] = return_policy
    item['seller_since'] = seller_since
    item['typical_response_time'] = typical_response_time

    item['period_of'] = period_of
    item['style_of'] = style_of
    item['origin'] = origin
    item['period'] = period
    item['creator'] = creator
    item['material'] = material

    item['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    item['category_id'] = category_id
    item['category2_id'] = category2_id
    item['category3_id'] = category3_id

    return item

def put_product(conn, cursor, item):
    sql = 'insert into product ' \
          '(product_id, title, price, status, period_of, style_of, origin, period, material, creator, \
          timestamp, category_id, category2_id, category3_id, dealer, dealer_location, dealer_link, \
            date_of_manufacture, `condition`, wear, number_of_items, dimensions, shipping, return_policy, \
            seller_since, typical_response_time) ' \
          'values (%d, "%s", %d, %d, "%s", "%s", "%s", "%s", "%s", "%s", "%s", %d, %d, %d, "%s", "%s", "%s", \
                    "%s", "%s", "%s", %d, "%s", "%s", "%s", "%s", "%s")' % \
                    (item['product_id'], item['title'], item['price'], item['status'],
                     item['period_of'], item['style_of'], item['origin'], item['period'],
                     item['material'], item['creator'], item['timestamp'],
                     item['category_id'], item['category2_id'], item['category3_id'],
                     item['dealer'], item['dealer_location'], item['dealer_link'],
                     item['date_of_manufacture'], item['condition'], item['wear'], item['number_of_items'],
                     item['dimension'], item['shipping'], item['return_policy'], item['seller_since'],
                     item['typical_response_time'])

    try:
        cursor.execute(sql)
    except pymysql.err.IntegrityError as e:
        logger.error(e)
        return False
    except Exception as e:
        logger.error(e)
        return False
    
    conn.commit()
    return True

def update_product(conn, cursor, product_id, price):
    sql = 'update product set price=%d where product_id=%d' % (product_id, price)

    try:
        cursor.execute(sql)
    except pymysql.err.IntegrityError as e:
        logger.error(e)
        return

    conn.commit()

def put_status(conn, cursor, product_id, product_price, product_status, product_link, product_image,
               category_id, category2_id, category3_id):

    logger.info('detect new status of product:[%d] ...', product_id)
    sql = 'insert into status (product_id, price, status, timestamp, link, image, category_id, category2_id, category3_id) ' \
        'values (%d, %d, %d, "%s", "%s", "%s" ,%d, %d, %d)' % \
        (product_id, product_price, product_status, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
         product_link, product_image,
         category_id, category2_id, category3_id)
    try:
        cursor.execute(sql)
    except pymysql.err.IntegrityError as e:
        return
    conn.commit()

def get_last_status(conn, cursor, product_id):

    sql = 'select status from status where product_id=%d order by id desc' % product_id
    cursor.execute(sql)
    status = cursor.fetchone()
    if not status:
        return -1

    return status[0]

def get_category3(conn, cursor):

    sql = 'select parent_id, id, link from category3'
    cursor.execute(sql)
    return cursor.fetchall()

def get_category(conn, cursor, link, category2_id, category3_id):

    sql = 'select parent_id from category2 where id=%d' % category2_id
    cursor.execute(sql)
    category_id = cursor.fetchone()[0]

    session = requests.session()

    html = session.get(link)
    sel = etree.HTML(html.text)
    total_pages = sel.xpath('//ul[@class="pagination-list cf"]/@data-tp')
    if total_pages:
        total_pages = int(total_pages[0])
    else:
        total_pages = 1

    logger.info('there are %d pages of this category to scrape', total_pages)
    get_page(conn, cursor, session, link, category_id, category2_id, category3_id)

    for i in range(2, total_pages + 1):
        page_url = link + '?page=%d' % i
        logger.info('scraping page:[%s] ...' % page_url)
        loop_enter = time.time()
        get_page(conn, cursor, session, page_url, category_id, category2_id, category3_id)
        loop_leave = time.time()
        logger.info('scraping page:[%s] cost %2f sec ...' % (page_url, loop_leave - loop_enter))

    session.close()

def main(loop_forever = False):

    usage = 'myprog -c <category3 id range>\nfor example: ./1stdibs.py -c 1,2\nthis is to scrape/monitor level-3 category range(1,2)'
    parser = OptionParser(usage)
    parser.add_option('-c', '--category', type='string', dest='category')
    options, args = parser.parse_args(sys.argv)
    if not options.category:
        parser.print_usage()
        return

    category_min = int(options.category.split(',') [0])
    category_max = int(options.category.split(',') [1])

    logger.warning('Connect MySQL via root:123456@localhost')

    conn = pymysql.connect(host = '127.0.0.1', user = 'root', password = '123456',
                    db = '1stdibs', charset = 'utf8')
    cursor = conn.cursor()

    while True:    

        logger.warning('scraping all categories ...')
        enter = time.time()
        items = get_category3(conn, cursor)

        for item in items:
            category3_id = item[1]
            if category3_id > category_max or category3_id <  category_min:
                continue;
            logger.info('scraping category:[%s] ...', item[2])
            loop_enter = time.time()
            get_category(conn, cursor, item[2], item[0], item[1])
            loop_leave = time.time()
            logger.info('scraping category:[%s] cost %2f sec', item[2], loop_leave - loop_enter)
        
        leave = time.time()
        logger.warning('scraping all categories done, cost %2f sec', leave - enter)

        if loop_forever:
            continue
        else:
            break        

    cursor.close()
    conn.close()

def test_get_url(start_url):
    conn = pymysql.connect(host = '127.0.0.1', user = 'root', password = '123456',
                    db = '1stdibs', charset = 'utf8')
    cursor = conn.cursor()

    session = requests.session()
    item = get_url(conn, cursor, session, start_url, 1, 1, 1)
    put_product(conn, cursor, item)
    #    put_status(conn, cursor, item['product_id'], item['price'], item['status'])
    print(item)
    cursor.close()
    conn.close()



if __name__ == '__main__':
    main()
    #test_get_url('https://www.1stdibs.com/furniture/seating/stools/set-of-five-contemporary-modern-industrial-style-bar-stools/id-f_8844993/')
