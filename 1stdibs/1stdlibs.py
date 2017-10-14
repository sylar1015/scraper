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

logger = logging.getLogger('SCRAPING-1STDLIBS')
fmt = '[%(name)s][%(levelname)s]:%(message)s'
h = logging.StreamHandler(sys.stdout)
h.setFormatter(logging.Formatter(fmt))
logger.addHandler(h)
logger.setLevel(logging.INFO)



def get_urls(link):

    base_url = 'https://www.1stdibs.com'
    start_url = link

    s = requests.session()
    html = s.get(start_url)
    sel = etree.HTML(html.text)
    urls = sel.xpath('//a[@class="product-link"]/@href')
    urls = [base_url + url for url in urls]

    total_pages = sel.xpath('//ul[@class="pagination-list cf"]/@data-tp')[0]
    total_pages = int(total_pages)

    for i in range(2, total_pages + 1):
        page_url = start_url + '?page=%d' % i
        html = s.get(page_url)
        sel = etree.HTML(html.text)
        page_urls = sel.xpath('//a[@class="product-link"]/@href')
        page_urls = [base_url + url for url in page_urls]
        urls.extend(page_urls)

    return urls

def main():

    urls = get_urls('https://www.1stdibs.com/furniture/storage-case-pieces/dry-bars/nouvelle-mirrored-bar/id-f_8676063/')

def get_url(conn, cursor, link, category_id, category2_id, category3_id):
    item = {}
    return item

if __name__ == '__main__':
    #main()
    #session = r
    # equests.session()
    #get_url(session, 'https://www.1stdibs.com/furniture/lighting/chandeliers-pendant-lights/striking-tom-greene-brutalist-torch-cut-chandelier-feldman-lighting/id-f_7980733/')

    html = open('url2.html').read()

    data = re.search('window.__SERVER_VARS__.data = (.*?);', html, re.S)
    data = data.group(1)
    data = json.loads(data)

    detailLinks = re.search('window.__SERVER_VARS__.detailLinks = (.*?);', html, re.S)
    detailLinks = detailLinks.group(1)
    detailLinks = json.loads(detailLinks)

    item = re.search('window.__SERVER_VARS__.item = (.*?);', html, re.S)
    item = item.group(1)
    item = json.loads(item)

    product_id = data['id']
    title = data['titleCondensed']
    price = data['retailPrice']['USD']
    status = 'Available'
    if data['isSold']:
        status = 'SOLD'
    elif data['isSuspended']:
        status = 'HOLD'

    print (product_id, title, price, status)

    print(detailLinks)
    period_of = detailLinks.get('periodOf', {})
    period_of = ', '.join(period_of.keys())

    origin = detailLinks.get('placeOfOrigin', {})
    if origin:
        for k, v in origin.items():
            origin = k
            break
    else:
        origin = ''

    period = detailLinks.get('period', {})
    if period:
        for k, v in period.items():
            period = k
            break
    else:
        period = ''

    creater = detailLinks.get('creator', {})
    if creater:
        for k, v in creater.items():
            creater = k
            break
    else:
        creater = ''

    material = detailLinks.get('materialsAndTechniques')
    if material:
        material = ', '.join(material.keys())
    else:
        material = ''

    style_of = detailLinks.get('styleOf', {})
    if style_of:
        for k, v in style_of.items():
            style_of = k
            break
    else:
        style_of = ''

    print (period_of,style_of, origin, period, creater, material)
