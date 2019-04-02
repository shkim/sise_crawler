#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests
import sys
import os
import re
import datetime
import time
import sqlite3

re_valid_date = re.compile(r'^(\d{4})\.(\d{2})\.(\d{2})$')
re_last_pagenum = re.compile(r'\?code=(\d+)&page=(\d+)$')

def normalize_date(dt):
    m = re_valid_date.match(dt)
    if not m:
        raise RuntimeError('Invalid date: ' + dt)

    year = int(m.group(1))
    month = int(m.group(2))
    mday = int(m.group(3))
    pdate = datetime.date(year, month, mday)
    return pdate

def normalize_comma_num(cnum):
    num = int(cnum.replace(',', ''))
    gen_cnum = "{:,}".format(num)
    if cnum != gen_cnum:
        raise RuntimeError('Invalid comma number: ' + cnum)
    return num

# for development
def get_cached_html(shcode):
    CACHE_FILENAME = "sample.html"
    if os.path.exists(CACHE_FILENAME):
        with open(CACHE_FILENAME, "r") as rfile:
            print("cache hit!")
            return rfile.read()

    page_url = "https://finance.naver.com/item/sise_day.nhn?code=" + shcode
    result = requests.get(page_url)
    if result.status_code == 200:
        with open(CACHE_FILENAME, "w") as cache_file:
            decoded = result.content.decode('euc-kr','replace')
            cache_file.write(decoded)
            print("cache file saved")
        return result.content


def get_html_content(shcode, page):
    page_url = "https://finance.naver.com/item/sise_day.nhn?code=%s&page=%d" % (shcode, page)
    result = requests.get(page_url)
    if result.status_code == 200:
        return result.content
    raise RuntimeError('Fetch failed: ' + page_url)

def get_last_page_num(html_content, shcode):
    soup = BeautifulSoup(html_content, 'html.parser')
    pgrr = soup.select_one(".pgRR a")
    m = re_last_pagenum.search(pgrr["href"])
    if not m:
        raise RuntimeError('Could not find last page link: ' + pgrr["href"])
    if shcode != m.group(1):
        raise RuntimeError('Code is different: {0} vs {1}'.format(shcode, m.group(1)))
    return int(m.group(2))

def parse_sise_page(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    head_row = soup.select_one("table.type2 tr")
    head_cols = list(map(lambda th: th.text, head_row.select('th')))
    if head_cols != ['날짜', '종가', '전일비', '시가', '고가', '저가', '거래량']:
        raise RuntimeError('Unexpected header columns: ' + head_cols)

    ret = []
    for drow in soup.select("table.type2 td[align=center]"):
        cols = drow.parent.select('td span')
        if len(cols) == 0:
            continue
        txts = list(map(lambda col: col.text.strip(), cols))
        day = normalize_date(txts[0])
        nums = list(map(lambda txt: normalize_comma_num(txt), txts[1:]))
        if len(nums) != 6:
            raise RuntimeError('Data column count is not 6: %d' % (len(nums)))
        ret.append([day] + nums)

    return ret


class db_persist:
    def __init__(self, code):
        self.code = code
        filename = 'daysise_{0}.sqlite'.format(code)
        self.conn = sqlite3.connect(filename)
        self.csr = self.conn.cursor()

        self.tbl_name = 'daily_{0}'.format(code)
        self.csr.execute('''
CREATE TABLE IF NOT EXISTS {0}
(
    DAY TEXT NOT NULL PRIMARY KEY,
    CLOSE INTEGER NOT NULL,
    CHANGE INTEGER NOT NULL,
    OPEN INTEGER NOT NULL,
    HIGH INTEGER NOT NULL,
    LOW INTEGER NOT NULL,
    VOLUME INTEGER NOT NULL
)'''.format(self.tbl_name))

    def close(self):
        self.conn.commit()
        self.conn.close()

    def insert_row(self, row):
        day = row[0].strftime('%Y-%m-%d')
        self.csr.execute('REPLACE INTO {0} VALUES(?, ?,?,?,?,?,?)'.format(self.tbl_name),
            (day, row[1],row[2],row[3],row[4],row[5],row[6]))

    def insert_rows(self, rows):
        for row in rows:
            self.insert_row(row)

    def dump_all(self):
        self.csr.execute('SELECT * FROM {0} ORDER BY DAY DESC'.format(self.tbl_name))
        for row in self.csr.fetchall():
            print(row)

def get_hms():
    return datetime.datetime.now().strftime('%H:%M:%S')

def run_initial_crawl(shcode, sleep_sec, verbose):
    print(get_hms(), "종목코드 '%s' 크롤링을 시작합니다." % (shcode))
    first_page_html = get_html_content(shcode, 1)
    last_page_num = get_last_page_num(first_page_html, shcode)
    print("맨 마지막 페이지는 %d 입니다." % (last_page_num))

    rows = parse_sise_page(first_page_html)    # test parsing
    print("최신 데이터 날짜는 %s 입니다." % (rows[0][0].isoformat()))

    localdb = db_persist(shcode)

    for page_num in range(1, last_page_num +1):
        time.sleep(sleep_sec)
        page_html = get_html_content(shcode, page_num)
        rows = parse_sise_page(page_html)
        num_rows = len(rows)
        if verbose:
            print(get_hms(), "[%s] 페이지 %d: %d rows (%s ~ %s)" % (shcode, page_num, num_rows, rows[0][0].isoformat(), rows[num_rows-1][0].isoformat()))
        localdb.insert_rows(rows)

    localdb.close()
    print(get_hms(), "종목코드 '%s' 크롤링을 마쳤습니다." % (shcode))

def main():
    shcode = ""
    if len(sys.argv) < 2:
        shcode = input("종목코드: ")
    else:
        shcode = sys.argv[1]
        if not str.isdigit(shcode):
            print("올바르지 않은 종목코드:", shcode)
            return

    if len(sys.argv) < 3 or sys.argv[2] != 'y':
        answer = input("종목코드 '%s' 크롤링을 진행하시겠습니까? [y/N]" % (shcode))
        if answer != 'y':
            print("중단합니다.")
            return

    run_initial_crawl(shcode, 3, True)
    print("Done!")

if __name__ == "__main__":
    #db_persist('069500').dump_all()
    main()
