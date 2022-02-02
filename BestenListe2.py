#!/usr/bin/python
import time
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import re
import argparse
import datetime
from bs4 import BeautifulSoup
import csv
from tqdm import tqdm
import concurrent.futures
import os
import sys
import asyncio


def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)


# parse the program args
parser = argparse.ArgumentParser()

parser.add_argument('-Sex', type=str, help='Geschlecht in "M","W" oder "X"')
parser.add_argument('-Bahn', type=str, help='Bahn "L" = 50m und "S" für 25m')
parser.add_argument('-full', action='store_true')
parser.add_argument('-AK', type=int, help='AltersKlasse')
parser.add_argument('-Style', type=str, help='Style zb "50B","100F" oder so')
parser.add_argument('-year', type=int, help='Startjahr der Saison; Wird year nicht gesetzt, so wird der Kader für das aktuelle Jahr bestimmt')

args = parser.parse_args()


class statextractor:

    def __init__(self):
        # Bahn, Lage, Platzierung, Name, Jahrgang, Verein, Zeit, Punkte, Ort, Datum
        self.kader_entries = []

    def makerequest(self, sex, Bahn, AK, style, seasonstart=None):

        # make the real request

        # determine wheter the Kader for the current year or a season shall be computed
        now = datetime.datetime.now()
        year = now.year
        timerange = '01.01.{0}|31.12.{0}'.format(year)
        if seasonstart:
            timerange = '01.06.{0}|31.05.{1}'.format(seasonstart, (seasonstart + 1))

        url = 'https://dsvdaten.dsv.de/Modules/Rankings/'

        # create the request session
        session = requests.Session()
        retry = Retry(total=6, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        # retreive viewstate and eventvalidation
        r1 = session.get(url)
        soup = BeautifulSoup(r1.content, features="html.parser")
        viewstate = soup.select("#__VIEWSTATE")[0]['value']
        eventvalidation = soup.select("#__EVENTVALIDATION")[0]['value']

        values = {'__EVENTTARGET': 'ctl00$ContentSection$_timerangeDropDownList',
                  '__EVENTARGUMENT': '',
                  '__LASTFOCUS': '',
                  '__VIEWSTATE': viewstate,
                  '__EVENTVALIDATION': eventvalidation,
                  'ctl00$ContentSection$hiddenTab': '#html',
                  'ctl00$ContentSection$_genderRadioButtonList': 'M',
                  'ctl00$ContentSection$_courseRadioButtonList': 'S',
                  'ctl00$ContentSection$_eventDropDownList': '100F|GL',
                  'ctl00$ContentSection$_timerangeDropDownList': timerange,
                  'ctl00$ContentSection$_ageDropDownList': '-1|-1',
                  'ctl00$ContentSection$_pointsDropDownList': 'Masters|2021|S',
                  'ctl00$ContentSection$_regionDropDownList': '0',
                  'ctl00$ContentSection$_searchTextBox': '',
                  'ctl00$ContentSection$_seasonDropDownList': '2021'
                  }
        # make a request for the right date
        r2 = session.post(url, data=values)

        soup = BeautifulSoup(r2.content, features="html.parser")
        viewstate = soup.select("#__VIEWSTATE")[0]['value']
        eventvalidation = soup.select("#__EVENTVALIDATION")[0]['value']

        values = {'__EVENTTARGET': 'ctl00$ContentSection$_submitButton',
                  '__EVENTVALIDATION': eventvalidation,
                  '__VIEWSTATE': viewstate,
                  'ctl00$ContentSection$hiddenTab': '#html',
                  'ctl00$ContentSection$_genderRadioButtonList': sex,
                  'ctl00$ContentSection$_courseRadioButtonList': Bahn,
                  'ctl00$ContentSection$_eventDropDownList': '{0}|GL'.format(style),
                  'ctl00$ContentSection$_timerangeDropDownList': timerange,
                  'ctl00$ContentSection$_ageDropDownList': '{0}|{1}'.format(year - AK, year - AK - 4),
                  'ctl00$ContentSection$_pointsDropDownList': 'Masters|2021|{0}'.format(Bahn),
                  'ctl00$ContentSection$_regionDropDownList': '0',
                  'ctl00$ContentSection$_seasonDropDownList': '2021',
                  }

        # request the actual data
        resp = requests.post(url, data=values)
        respdata = resp.content.decode('utf-8')
        respdata = respdata.replace('\r', '')

        # find all the swimmerdata in the html
        pattern = re.compile(
            '<td>(.*?)</td>\n *<td>(.*?)</td>\n *<td>([0-9]{4}|&nbsp;)</td>\n *<td>(.*?)</td>\n *<td>(.*?)</td>\n *<td>(.*?)</td>\n *<td>(.*?)</td>\n '
            '*<td>(.*?)</td>')

        # find html blocks with swimmer info
        tophundret = [x for x in re.finditer(pattern, respdata)]

        # get all the Clubs listed in Bavaria
        r1 = requests.get('https://dsvdaten.dsv.de/Modules/Clubs/Index.aspx?StateID=2#clubs')
        r1_data = r1.content.decode('utf-8')
        r1_data = r1_data.replace('\r', '')
        # find all Club entries using regex
        pattern = re.compile('<td>\n.*? <a href=.*?>(.*)</a>\n.*?</td>')

        # take the fist and only group because that is where the Club name is stored
        alleVereine = [x.group(1) for x in re.finditer(pattern, r1_data)]
        # make alle Verine a set to better search in with 'not in' operation
        alleVereine = set(alleVereine)
        swimmersdata = []

        for swimmer in tophundret:

            # alle nicht bayerischen Vereine werden entfernt
            if swimmer.group(4) not in alleVereine:
                continue

            # falls ein schwimmer in bayern schwimmt wird die gesamte info gespeichert
            swimmersdata.append([swimmer.group(1), swimmer.group(2), swimmer.group(3), swimmer.group(4), swimmer.group(5), swimmer.group(6),
                                 swimmer.group(7), swimmer.group(8)])

        # only some of the top ten are eligible for the Kader
        swimmersdata = swimmersdata[:10]

        Kader = []
        # top 5 swimmers are automaticly accepted
        for swimmerdata in swimmersdata[0:5]:
            Kader.append([Bahn, style, AK] + swimmerdata)

        # next 5 are only accepted if they have more than 700 points
        for swimmerdata in swimmersdata[5:10]:
            if int(swimmerdata[5]) > 700:
                Kader.append([Bahn, style, AK] + swimmerdata)

        return Kader

    def createKaderList(self):

        disciplines = ['50F', '100F', '200F', '400F', '800F', '1500F',
                       '50B', '100B', '200B',
                       '50R', '100R', '200R',
                       '50S', '100S', '200S',
                       '100L', '200L', '400L']
        sexes = ['M', 'W']
        Bahnen = ['L', 'S']
        AKs = list(range(20, 105, 5))

        year = args.year

        my_iter = []
        for sex in sexes:
            for Bahn in Bahnen:
                for AK in AKs:
                    for style in disciplines:
                        my_iter.append([sex, Bahn, AK, style, year])

        Kader = []

        with tqdm(total=len(my_iter)) as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = []

                for reqargs in my_iter:
                    futures.append(executor.submit(self.makerequest, reqargs[0], reqargs[1], reqargs[2], reqargs[3], reqargs[4]))
                    time.sleep(0.5)
                    pbar.update(1)

                for future in concurrent.futures.as_completed(futures):
                    minikader = future.result()
                    Kader.extend(minikader)

        with open('Kader.csv', 'w') as csvfile:

            csvwriter = csv.writer(csvfile, delimiter=';')
            csvwriter.writerow(['Bahn', 'Style', 'AK', 'Platz_Dt', 'Name', 'Jahrgang', 'Verein', 'Zeit', 'Punkte', 'Ort', 'Datum'])
            for i in Kader:
                csvwriter.writerow(i)


def main():
    requestkader = statextractor()

    if args.full:
        requestkader.createKaderList()

    elif args.Sex and args.Bahn and args.AK and args.Style:
        if args.year:
            year = args.year
        else:
            year = None

        partialkader = requestkader.makerequest(args.Sex, args.Bahn, args.AK, args.Style, year)

        with open('TeilKader_AK{0}_{1}_{2}_{3}.csv'.format(args.AK, args.Style, args.Sex, args.Bahn), 'w') as csvfile:

            csvwriter = csv.writer(csvfile, delimiter=';')
            csvwriter.writerow(['Bahn', 'Style', 'AK', 'Platz', 'Name', 'Jahrgang', 'Verein', 'Zeit', 'Punkte', 'Ort', 'Datum'])
            for i in partialkader:
                csvwriter.writerow(i)

    else:
        print('Please add more arguments:')
        print('Either add "-full" to obtain the entrie List')
        print('Or add -Sex, -Bahn, -Style and -AK')
        print('Add -help for further info')


if __name__ == '__main__':
    main()


#pyinstaller -F  BestenListe.py
