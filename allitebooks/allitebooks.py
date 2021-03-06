#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import csv
import requests
from lxml import etree
from optparse import OptionParser

def scrape_books_per_page(sel):

    books = []

    items = sel.xpath('//div[@class="entry-body"]//a[@rel="bookmark"]')
    for item in items:
        title = item.xpath('./text()')[0]
        link = item.xpath('./@href')[0]
        books.append((title, link))

    return books

def scrape_books(keyword, output):

    base_url = 'http://www.allitebooks.com/?s='

    session = requests.session()
    html = session.get(base_url + keyword)
    sel = etree.HTML(html.text)
    books = scrape_books_per_page(sel)
    

    max_page = int(sel.xpath('//div[@class="pagination clearfix"]/a/text()')[-1])

    for i in range(2, max_page + 1):
        start_url = 'http://www.allitebooks.com/page/%d/?s=%s' % (i, keyword)
        html = session.get(start_url)
        sel = etree.HTML(html.text)
        books.extend(scrape_books_per_page(sel))

    with open(output, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(books)

def download_book(link):
    html = requests.get(link)
    book_name = link.split('/')[-1]
    with open(book_name, 'wb') as f:
        f.write(html.content)

def main():
    
    usage = 'myprog -k <keyword> [-o <filename>]'
    parser = OptionParser(usage)
    parser.add_option('-k', '--keyword', type='string', dest='keyword')
    parser.add_option('-o', '--output', type='string', dest='output', default='result.csv')
    parser.add_option('-d', '--download', type='string', dest='download')
    options, args = parser.parse_args(sys.argv)

    if options.keyword:
        print ('scraping keyword[%s] to file[%s]' % (options.keyword, options.output))
        scrape_books(options.keyword, options.output)
        return
    elif options.download:
        print ('downloading [%s]' % options.download.split('/')[-1])
        download_book(options.download)
        return
    else:
        parser.print_usage()

if __name__ == '__main__':
    main()
