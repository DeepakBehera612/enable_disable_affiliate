import requests
import pymongo
import json
import pyodbc
import logging
import time
from threading import Thread
import sys
from termcolor import colored, cprint
import xml.dom.minidom
import xml.etree.ElementTree as ET


try:
    logging.basicConfig(filename='Logs/affiliate.log', level=logging.INFO,
                        format='%(levelname)s: %(asctime)s : %(message)s', datefmt='%d-%m-%Y : %H:%M:%S')
    print('Process Starting Please Wait ...')
    logging.info('Process Stating Please Wait ...')
    while True:
        mytree = ET.parse('advertisers.xml')
        root = mytree.getroot()
        arr = root.attrib

        tempList = []
        for ff in root.findall('advertiser'):
            deee = ff.attrib['Enable'].split()
            tempList.append(deee)

        print(tempList)
        pids = []
        for c in root.findall('advertiser'):
            is_enable = c.attrib['Enable']
            if is_enable == 'true':
                setid = c.find('set_id').text
                pids = c.find('pub_id').text.split(',')
                con = pyodbc.connect(
                    'DRIVER={SQL Server};SERVER=(local);DATABASE=your_database_name;UID=your-username;PWD=yourpassword')

                query = con.cursor()
                Isexit_pubid = con.cursor()
                query.execute(
                    f"select * from OfferLog where AdvertiserSetId='{setid}' and [Status]=1")
                sql_data = query.fetchall()
                offer_found = len(sql_data)

                logging.info(
                    f"Total Active OfferId's Found : {offer_found} : for SetId : {setid}")
                cprint(
                    f"Total Active OfferId's Found : {offer_found} : for SetId : {setid}", 'green')

                for offer_id in sql_data:
                    for pid in pids:
                        url = "http://api.mobtions.affise.com/3.0/offer/disable-affiliate"

                        params = {
                        }
                        payload = {
                            "offer_id": offer_id[0],
                            "pid": pid,
                            "notice": 0
                        }
                        headers = {
                            'API-Key': 'your-api-key'
                        }
                        response = requests.request(
                            "POST", url, headers=headers, data=payload)
                        data = json.loads(response.text.encode('utf8'))
                        logging.info(
                            f'Affiliate was enabled OfferId : {offer_id[0]} SetId:{offer_id[2]} : and pid {pid}')

                        cprint(
                            f'info : Affiliate was enabled OfferId : {offer_id[0]} SetId : {offer_id[2]} : and pid {pid}')

                        temp = f"select * from EnableAffiliates where OfferId ='{offer_id[0]}' and PubId='{pid}'"
        cprint(
            'one pharse done, wait for 1 minutes for next scedule...', 'yellow')
        logging.info(
            'one pharse done, wait for 1 minutes for next scedule...')
        time.sleep(60)
except Exception as ex:
    # logging.warning('something went to wrong...', ex)
    print('something went to wrong...', ex)
