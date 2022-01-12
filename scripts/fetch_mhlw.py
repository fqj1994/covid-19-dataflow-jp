# Based on https://github.com/owid/covid-19-data/blob/db830e448607de47c8430f9122bcd72b0acce4fd/scripts/src/cowidev/hosp/sources/japan.py

import datetime
import re
import requests
import sys
import tabula

import pandas as pd

from bs4 import BeautifulSoup

METADATA = {
    "source_url": "https://www.mhlw.go.jp/stf/seisakunitsuite/newpage_00023.html",
    "source_url_ref": "https://www.mhlw.go.jp/stf/seisakunitsuite/newpage_00023.html",
    "source_name": "Ministry of Health, Labour and Welfare",
    "entity": "Japan",
}

DATE_RE = re.compile(r"(20[0-9]*?)年(.*?)月(.*?)日")

def handle_highcare_note(data: str) -> str:
    assert '注' in data
    m = re.search(r'([0-9]*)\([0-9]', data)
    assert m
    return m.group(1)

def process_file(url: str, date: str, debug: bool) -> dict:
    hospitalized_col = None
    critical_col = None
    if url.endswith('.pdf'):
        tables = tabula.read_pdf(
                url, pages=1, silent=True, lattice=True, multiple_tables=True)
        max_len = 0
        for table in tables:
            if len(table) > max_len:
                df = table
                max_len = len(table)
        df = df.reset_index().T.reset_index().T
    else:
        df = pd.read_excel(url)

    for col in df:
        for obj in df[col][0:10]:
            if type(obj) is not str:
                continue
            obj = obj.replace('\n', '').replace('\r', '').replace(' ', '')
            if hospitalized_col is None and "入院者数" in obj:
                hospitalized_col = df[col]
            if critical_col is None and "うち重症者数" in obj:
                critical_col = df[col]
            # In these dates, tabula failed to recognize the right column header.
            if date >= '2020-08-26' and date <= '2020-11-11':
                if '最終フェーズにおける即応病床(計画)数(注5)' in obj:
                    critical_col = df[col]
            if date == '2020-05-13':
                if '(4)自宅療養者数' == obj:
                    critical_col = df[col]
            if date >= '2020-11-18' and date <= '2020-12-16':
                if '確保病床数(注4)' == obj:
                    critical_col = df[col]

    ret = []

    if hospitalized_col is not None:
        hospitalized_col = hospitalized_col.dropna()
        hospitalized = str(hospitalized_col.iloc[-1]).replace(',', '')
        try:
            ret.append({
                "date": date,
                "indicator": "Daily hospital occupancy",
                "value": int(hospitalized),
                "entity": METADATA["entity"],
            })
        except ValueError:
            pass
    
    if critical_col is not None:
        critical_col = critical_col.dropna()
        critical = str(critical_col.iloc[-1]).replace(',', '')
        if date in ['2020-08-26' , '2020-09-16']:
            critical = handle_highcare_note(critical)
        try:
            ret.append({
                "date": date,
                "indicator": "Daily ICU occupancy",
                "value": int(critical),
                "entity": METADATA["entity"],
            })
        except ValueError:
            pass


    if debug:
        print(date, url, '\n', ret)
    return ret


def main(debug=False):
    response = requests.get(METADATA["source_url"]).content.decode("utf-8")
    soup = BeautifulSoup(response, features='lxml')

    groups = []
    group = []
    for li in soup.select('.m-grid li'):
        if DATE_RE.search(str(li)):
            if group:
                groups.append(''.join(group))
            group = []
        group.append(str(li))
    if group:
        groups.append(''.join(group))

    records = []
    for report in reversed(groups):
        if '新型コロナウイルス感染症患者の療養状況' not in report:
            continue
        year, month, day  = DATE_RE.search(report).groups()
        links = sorted([a.get('href') for a in BeautifulSoup(report, features='lxml').findAll('a')])
        if links[-1].endswith('.xlsx'):
            path = links[-1]
        else:
            path = links[0]
        date = str(datetime.date(year=int(year), month=int(month), day=int(day)))
        records.extend(process_file("https://www.mhlw.go.jp" + path, date, debug))

    df = pd.DataFrame.from_records(records)
    return df, METADATA


if __name__ == "__main__":
    if 'v' in sys.argv[1:]:
        print(main(debug=True)[0].to_string())
    else:
        print(main()[0].to_csv())
