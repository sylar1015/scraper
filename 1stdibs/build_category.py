#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import pymysql
import requests
from lxml import etree
import logging
import sys
import time

logger = logging.getLogger('BUILDING-CATEGORY')
fmt = '[%(name)s][%(levelname)s]:%(message)s'
h = logging.StreamHandler(sys.stdout)
h.setFormatter(logging.Formatter(fmt))
logger.addHandler(h)
logger.setLevel(logging.INFO)

categories = [
    ('Furniture', 'https://www.1stdibs.com/furniture/'),
    ]

def build_category3(conn, cursor, session, name, link, parent_id):

    sql = 'select id from category3 where name="%s"' % name
    cursor.execute(sql)
    category3 = cursor.fetchone()
    if category3:
        return
    else:
        sql = 'insert into category3 (name, link, parent_id) values ("%s", "%s", %d)'\
              % (name, link, parent_id)
        cursor.execute(sql)
        conn.commit()

def build_category2(conn, cursor, session, name, link, parent_id):

    sql = 'select id from category2 where name="%s"' % name
    cursor.execute(sql)
    category2 = cursor.fetchone()
    category2_id = 1
    if category2:
        category2_id = category2[0]
    else:
        sql = 'insert into category2 (name, link, parent_id) values ("%s", "%s", %d)'\
              % (name, link, parent_id)
        cursor.execute(sql)
        conn.commit()
        sql = 'select id from category2 where name="%s"' % name
        cursor.execute(sql)
        category2_id = cursor.fetchone()[0]

    html = session.get(link)
    sel = etree.HTML(html.text)
    items = sel.xpath('//div[@class="nested-facet-nav-items is-nested"]//a')
    for item in items:
        link3 = 'https://www.1stdibs.com' + item.xpath('./@href')[0]
        name3 = item.xpath('./span/text()')[0].strip('\n ')
        if name == name3:
            continue
        build_category3(conn, cursor, session, name3, link3, category2_id)

def build_category(conn, name, link):
    cursor = conn.cursor()
    sql = 'select id from category where name="%s"' % name
    cursor.execute(sql)

    parent = cursor.fetchone()
    category_id = 1
    if parent:
        category_id = parent[0]
    else:
        sql = 'insert into category (name, link) values ("%s", "%s")' % (name, link)
        cursor.execute(sql)
        conn.commit()
        sql = 'select id from category where name="%s"' % name
        cursor.execute(sql)
        parent = cursor.fetchone()
        category_id = parent[0]

    session = requests.session()
    html = session.get(link)

    sel = etree.HTML(html.text)
    items = sel.xpath('//div[@class="nested-facet-nav-items is-nested"]//a')
    for item in items:
        link = 'https://www.1stdibs.com' + item.xpath('./@href')[0]
        name = item.xpath('./span/text()')[0].strip('\n ')
        build_category2(conn, cursor, session, name, link, category_id)

def main():

    logger.info('Connect MySQL via root:123456@localhost')
    enter = time.time()

    conn = pymysql.connect(host = '127.0.0.1', user = 'root', password = '123456',
                    db = '1stdibs', charset = 'utf8')

    for category in categories:
        building_enter = time.time()
        logger.info('Building (%s) ...', category[0])
        build_category(conn, category[0], category[1])
        building_leave = time.time()
        logger.info('Building (%s) cost %2f sec', category[0], building_leave - building_enter)

    conn.close()

    leave = time.time()
    logger.info('Success, cost %2f sec', leave - enter)

if __name__ == '__main__':
    main()