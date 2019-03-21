#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import configparser
import mysql.connector as mysql
import sqlite3
import datetime
import sys

DIFF_THRESHOLD = 2

class SourceDB:
    def __init__(self, code):
        filename = 'daysise_{0}.sqlite'.format(code)
        self.code = code
        self.conn = sqlite3.connect(filename)
        self.csr = self.conn.cursor()
        self.tbl_name = 'daily_{0}'.format(code)

    def close(self):
        self.conn.close()

    def load_all(self):
        self.csr.execute('SELECT DAY,CLOSE,CHANGE,OPEN,HIGH,LOW,VOLUME FROM {0} ORDER BY DAY'.format(self.tbl_name))
        self.rows = []
        for src in self.csr.fetchall():
            day = datetime.datetime.strptime(src[0], '%Y-%m-%d').date()
            row = {'DAY': day, 'CLOSE': src[1], 'CHANGE': src[2], 'OPEN': src[3], 'HIGH': src[4], 'LOW': src[5], 'VOLUME': src[6]}
            self.rows.append(row)

        diff_count = 0
        for i in range(1, len(self.rows)):
            c0 = self.rows[i - 1]['CLOSE'] 
            c1 = self.rows[i]['CLOSE'] 
            change = abs(c1 - c0)
            expect = self.rows[i]['CHANGE']
            if change != expect:
                diff = abs(expect - change)
                perc = 100.0 * float(diff) / float(self.rows[i]['CLOSE'])
                print('%s ~ %s: Expected change:%d, actual:%d, diff=%d(%.2f%%)' % (self.rows[i - 1]['DAY'], self.rows[i]['DAY'], expect, change, diff, perc))
                diff_count += 1
                if perc > DIFF_THRESHOLD:
                    raise RuntimeError("Too big CHANGE difference: %d(%.2f%%) near %s" % (diff, perc, self.rows[i]['DAY']))

        return len(self.rows), diff_count

    def get_rows(self):
        return self.rows


MYSQL_TABLE_NAME = 'daysise'

class TargetDB:
    def __init__(self, config_filename):
        config = configparser.ConfigParser()
        config.read(config_filename)
        dbconf = config['mysql']
        self.conn = mysql.connect(**dbconf)
        self.csr = self.conn.cursor()
        self.csr.execute("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s LIMIT 1", (dbconf['database'], MYSQL_TABLE_NAME))
        if len(self.csr.fetchall()) == 0:
            self.csr.execute('''
CREATE TABLE daysise
(
    DAY DATE NOT NULL,
    CODE VARCHAR(8) NOT NULL,
    O INTEGER NOT NULL,
    H INTEGER NOT NULL,
    L INTEGER NOT NULL,
    C INTEGER NOT NULL,
    V INTEGER NOT NULL,

    PRIMARY KEY (DAY,CODE)
)
                ''')

    def close(self):
        self.conn.commit()
        self.conn.close()

    def insert_all(self, src):
        cnt = 0
        for row in src.get_rows():
            self.csr.execute('REPLACE INTO {0} (DAY,CODE, O,H,L,C,V) VALUES(%s,%s, %s,%s,%s,%s,%s)'.format(MYSQL_TABLE_NAME),
                (row['DAY'], src.code, row['OPEN'], row['HIGH'], row['LOW'], row['CLOSE'], row['VOLUME']))
            print(row)
            cnt += 1
        return cnt


def main():
    shcode = ""
    if len(sys.argv) < 2:
        shcode = input("종목코드: ")
    else:
        shcode = sys.argv[1]
        if not str.isdigit(shcode):
            print("올바르지 않은 종목코드:", shcode)
            return

    if len(sys.argv) < 3:
        cfgfile = input("설정파일 [config.ini]: ")
        if not cfgfile:
            cfgfile = 'config.ini'
    else:
        cfgfile = sys.argv[2]

    if len(sys.argv) < 4 or sys.argv[3] != 'y':
        answer = input("종목코드 '%s' 시세 정보를 MySQL(설정파일:%s)로 복사하시겠습니까? [y/N]" % (shcode, cfgfile))
        if answer != 'y':
            print("중단합니다.")
            return

    src = SourceDB(shcode)
    num_rows, diff_cnt = src.load_all()
    if num_rows == 0:
        raise RuntimeError("Source DB has no rows")

    if diff_cnt > 0:
        if input('Inconsistency found(%d). Proceed to insert into MySQL? [y/N]' % diff_cnt) != 'y':
            print('Aborted')
            return

    dst = TargetDB(cfgfile)
    num_inserted = dst.insert_all(src)

    src.close()
    dst.close()
    print("Done", num_inserted)

if __name__ == "__main__":
    main()
