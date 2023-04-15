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
    pids = [775, 747, 772, 691, 761, 448, 591, 545, 829, 181, 859, 465, 16, 695, 730, 754, 900, 331, 83, 813, 896, 735, 579, 97, 532, 589, 634, 210, 69, 144, 288, 849,
            141, 512, 199, 185, 905, 707, 499, 642, 878, 755, 736, 853, 332, 882, 429, 379, 684, 891, 631, 903, 894, 910, 644, 847, 205, 192, 844, 774, 43, 846, 197, 920]
    logging.basicConfig(filename='Logs/affiliate.log', level=logging.INFO,
                        format='%(levelname)s: %(asctime)s : %(message)s', datefmt='%d-%m-%Y : %H:%M:%S')
    print('Process Starting Please Wait ...')
    logging.info('Process Stating Please Wait ...')
    affise_key = 'your-api-key'
    addOfferId = []
    Advertiser_id = 'your-adv-id'
    url = f"http://api.mobtions.affise.com/3.0/offers?advertiser[0]={Advertiser_id}&status[0]=active"
    payload = {}
    headers = {
        'API-Key': affise_key
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    jsonRes = response.json()
    for item in jsonRes['offers']:
        # print(item['id'], sep=',')
        addOfferId.append(item['id'])
    total_id_found = len(addOfferId)
    cprint(
        f"Total Active OfferId's Found : {total_id_found} : for AdvId : {Advertiser_id}", 'green')
    logging.info(
        f"Total Active OfferId's Found : {total_id_found} : for AdvId : {Advertiser_id}")
    while True:
        for offer_id in addOfferId:
            for pid in pids:
                enable_url = "http://api.mobtions.affise.com/3.0/offer/enable-affiliate"
                params = {
                }
                payload = {
                    "offer_id": offer_id,
                    "pid": pid,
                    "notice": 0
                }
                headers = {
                    'API-Key': affise_key
                }
                response = requests.request(
                    "POST", enable_url, headers=headers, data=payload)
                data = json.loads(response.text.encode('utf8'))
                logging.info(
                    f'Affiliate was enabled OfferId : {offer_id} : and pid {pid}')

                cprint(
                    f'info : Affiliate was enabled OfferId : {offer_id} : and pid {pid}')
        logging.info(
            'one pharse done, wait for 1 minutes for next scedule...')
        cprint("All OfferId's updated", 'green')
        cprint("one pharse done, wait for 1 minutes for next scedule...", 'yellow')
        time.sleep(60)

except Exception as ex:
    cprint('something went to wrong...', 'red', ex)
    #logging.error('something went to wrong...', ex)
