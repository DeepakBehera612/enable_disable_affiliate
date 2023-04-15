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
    pids = [192, 386, 43]
    logging.basicConfig(filename='Logs/affiliate.log', level=logging.INFO,
                        format='%(levelname)s: %(asctime)s : %(message)s', datefmt='%d-%m-%Y : %H:%M:%S')
    cprint('Process starting please wait ...', 'yellow')
    logging.info('Process stating please wait ...')
    Advertise_SetId = 'Instal_CPI_API'
    affise_key = 'your-api-key'
    addOfferId = [914961, 914962, 914963, 914964,
                  914965, 914966, 914967, 914968, 914969, 914970]
    total_id_found = len(addOfferId)
    total_affiliateid_found = len(pids)
    cprint(
        f"Total Active OfferId's Found : {total_id_found} : Advertiser :  {Advertise_SetId}", 'green')
    logging.info(
        f"Total Active OfferId's Found : {total_id_found} : Advertiser {Advertise_SetId}")
    logging.info(
        f"Total affiliate Id's Found : {total_affiliateid_found} : Advertiser :  {Advertise_SetId}")
    cprint(
        f"Total affiliate Id's Found : {total_affiliateid_found} : Advertiser :  {Advertise_SetId}", 'green')
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
                    f'Affiliate was enabled OfferId : {offer_id} : Advertiser : {Advertise_SetId} : and pid {pid}')

                cprint(
                    f'info : Affiliate was enabled OfferId : {offer_id} : Advertiser : {Advertise_SetId} : and pid {pid}')
        logging.info(
            'one pharse done, wait for 5 minutes for next scedule...')
        cprint(
            f"All OfferId's updated for Advertiser : {Advertise_SetId}", 'green')
        logging.info("All OfferId's updated")
        cprint("one pharse done, wait for 5 minutes for next scedule...", 'yellow')
        time.sleep(300)

except Exception as ex:
    cprint('something went to wrong...', 'red', ex)
    #logging.error('something went to wrong...', ex)
