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
logger.setLevel(logging.INFO)

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
        if is_new_product(conn, cursor, product_id):
            logger.info('scraping product:[%s] ...', product_link)
            loop_enter = time.time()
            product = get_url(conn, cursor, session, product_link, category_id, category2_id, category3_id)
            loop_leave = time.time()
            logger.info('scraping product:[%s] cost %2f sec ...', product_link, loop_leave - loop_enter)
            put_product(conn, cursor, product)
            put_status(conn, cursor, product_id, product['price'], product['status'],
                       category_id, category2_id, category3_id)
        else:
            product_price = item.xpath('./span[@class="product-price"]/span/@data-usd')
            product_status = 0
            if product_price:
                product_price = product_price[0]
                product_price = [x for x in product_price if x.isdigit()]
                product_price = int(''.join(product_price))
            else:
                product_price = 0
                if item.xpath('./span[@class="product-price"]/span/@data-hide-price'):
                    #hide price, consider on sale
                    pass
                else:
                    product_status = item.xpath('./span[@class="product-price"]/span/@data-hold')
                    if product_status:
                        product_status = 1
                    else:
                        product_status = 2
            last_status = get_last_status(conn, cursor, product_id)
            if last_status != product_status:
                put_status(conn, cursor, product_id, product_price, product_status,
                           category_id, category2_id, category3_id)

def get_url(conn, cursor, session, link, category_id, category2_id, category3_id):

    item = {}

    html = session.get(link)

    data = re.search('window.__SERVER_VARS__.data = (.*?\\});', html.text, re.S)
    data = data.group(1)
    data = json.loads(data)

    detailLinks = re.search('window.__SERVER_VARS__.detailLinks = (.*?\\});', html.text, re.S)
    
    detailLinks = detailLinks.group(1)
    try:
        detailLinks = json.loads(detailLinks)
    except Exception as e:
        detailLinks = {}

    # data_item = re.search('window.__SERVER_VARS__.item = (.*?);', html.text, re.S)
    # data_item = data_item.group(1)
    # data_item = json.loads(data_item)

    product_id = data['id']
    title = data['titleCondensed'].replace('"','')
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

    creator = detailLinks.get('creator', {})
    creator = ', '.join(creator)

    material = detailLinks.get('materialsAndTechniques', {})
    material = ', '.join(material)

    style_of = detailLinks.get('styleOf', {})
    style_of = ', '.join(style_of)

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
          timestamp, category_id, category2_id, category3_id) ' \
          'values (%d, "%s", %d, %d, "%s", "%s", "%s", "%s", "%s", "%s", "%s", %d, %d, %d)' % \
                    (item['product_id'], item['title'], item['price'], item['status'],
                     item['period_of'], item['style_of'], item['origin'], item['period'],
                     item['material'], item['creator'], item['timestamp'],
                     item['category_id'], item['category2_id'], item['category3_id'])

    cursor.execute(sql)
    conn.commit()

def put_status(conn, cursor, product_id, product_price, product_status,
               category_id, category2_id, category3_id):

    logger.info('detect new status of product:[%d] ...', product_id)
    sql = 'insert into status (product_id, price, status, timestamp, category_id, category2_id, category3_id) ' \
        'values (%d, %d, %d, "%s", %d, %d, %d)' % \
        (product_id, product_price, product_status, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
         category_id, category2_id, category3_id)
    cursor.execute(sql)
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
    total_pages = sel.xpath('//ul[@class="pagination-list cf"]/@data-tp')[0]
    total_pages = int(total_pages)

    logger.info('there are %d pages of this category to scrape', total_pages)
    get_page(conn, cursor, session, link, category_id, category2_id, category3_id)

    for i in range(total_pages + 1):
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

    logger.info('Connect MySQL via root:123456@localhost')

    conn = pymysql.connect(host = '127.0.0.1', user = 'root', password = '123456',
                    db = '1stdibs', charset = 'utf8')
    cursor = conn.cursor()

    while True:    

        logger.info('scraping all categories ...')
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
        logger.info('scraping all categories done, cost %2f sec', leave - enter)

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
    print(item)
    cursor.close()
    conn.close()



if __name__ == '__main__':
    main()
    #test_get_url('https://www.1stdibs.com/furniture/lighting/chandeliers-pendant-lights/anton-fogh-holm-alfred-j-andersen-enameled-steel-double-pendant-light/id-f_8201523/')
