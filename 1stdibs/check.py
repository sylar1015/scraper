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

def get_page(conn, cursor, session, link):

    base_url = 'https://www.1stdibs.com'
    html = session.get(link)
    sel = etree.HTML(html.text)
    items = sel.xpath('//div[@class="product-container"]')
    
    normal = 0
    total = len(items)
    error = 0
    new = 0

    for item in items:
        product_link = item.xpath('./a/@href')
        product_id = 0
        if not product_link:
            error += 1
            continue
        product_link = product_link[0]
        product_id = int(product_link[product_link.find('id-f_') + 5: -1])
        product_link = base_url + product_link
        if is_new_product(conn, cursor, product_id):
            logger.info('scraping product:[%s] ...', product_link)
            loop_enter = time.time()
            product = get_url(conn, cursor, session, product_link)
            loop_leave = time.time()
            logger.info('scraping product:[%s] cost %2f sec ...', product_link, loop_leave - loop_enter)
            #add exception handler
            if not product:
                error += 1
                logger.error('parse product(%s, %s) failed ...', product_id, product_link)
            else:
                new += 1
                logger.error('new product(%s, %s)', product_id, product_link)
        else:
            normal += 1
            logger('product (%s, %s) already in database', product_product_link)
            

    return normal, total, new, error

def get_url(conn, cursor, session, link):

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

def main(loop_forever = False):

    usage = './check.py -u link'
    parser = OptionParser(usage)
    parser.add_option('-u', '--url', type='string', dest='url')
    options, args = parser.parse_args(sys.argv)
    if not options.url:
        parser.print_usage()
        return

    link = options.url

    logger.warning('Connect MySQL via root:123456@localhost')

    conn = pymysql.connect(host = '127.0.0.1', user = 'root', password = '123456',
                    db = '1stdibs', charset = 'utf8')
    cursor = conn.cursor()

    session = requests.session()

    html = session.get(link)
    sel = etree.HTML(html.text)
    total_pages = sel.xpath('//ul[@class="pagination-list cf"]/@data-tp')
    if total_pages:
        total_pages = int(total_pages[0])
    else:
        total_pages = 1

    logger.info('there are %d pages of this category to scrape', total_pages)
    normal, total, new, error = get_page(conn, cursor, session, link)

    for i in range(2, total_pages + 1):
        page_url = link + '?page=%d' % i
        logger.info('scraping page:[%s] ...' % page_url)
        loop_enter = time.time()
        v1, v2, v3, v4 = get_page(conn, cursor, session, page_url)
        normal += v1
        total += v2
        new += v3
        error += v4
        loop_leave = time.time()
        logger.info('scraping page:[%s] cost %2f sec ...' % (page_url, loop_leave - loop_enter))
        logger.info('%d/%d, %d, %d', v1, v2, v3, v4)

    session.close()
    logger.info('statistic: %d, %d, %d, %d', normal, total, new, errr)

    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
