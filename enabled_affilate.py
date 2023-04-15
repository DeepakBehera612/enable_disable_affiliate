import requests
import pymongo
import json
import pyodbc
import logging
import time
from threading import Thread
from setids import set_ids_task1, set_ids_task2
import sys
from termcolor import colored, cprint
from pids import pids
import threading


class Task1(Thread):
    def run(self):
        try:
            logging.basicConfig(filename='Logs/affiliate.log', level=logging.INFO,
                                format='%(levelname)s: %(asctime)s : %(message)s', datefmt='%d-%m-%Y : %H:%M:%S')
            print('Process Starting Please Wait ...')
            logging.info('Process Stating Please Wait ...')
            while True:
                for setid in set_ids_task1:
                    con = pyodbc.connect(
                        'DRIVER={SQL Server};SERVER=(local);DATABASE=mobtions;UID=sa;PWD=deepak')

                    query = con.cursor()
                    query.execute(
                        f"select * from OfferLog where AdvertiserSetId='{setid}' and [Status]=1")
                    sql_data = query.fetchall()
                    offer_found = len(sql_data)

                    logging.info(
                        f"Total Active OfferId's Found : {offer_found} : for SetId : {setid}")
                    cprint(
                        f"Total Active OfferId's Found : {offer_found} : for SetId : {setid}", 'green')
                    # pids = [2]

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
                                'API-Key': '446jh787b5711005545j4j4kvj546b465456kj54655k80990cvbc89ffg8e6c7d3d3'
                            }
                            response = requests.request(
                                "POST", url, headers=headers, data=payload)
                            # data = json.loads(response.text.encode('utf8'))
                            logging.info(
                                f'Affiliate was enabled OfferId : {offer_id[0]} SetId:{offer_id[2]} : and pid {pid}')

                            print(
                                f'info : Affiliate was enabled OfferId : {offer_id[0]} SetId : {offer_id[2]} : and pid {pid}')
                            # if offer_id[0] != None:
                            #     pass
                            # query.execute(
                            #     f'''INSERT INTO EnableAffiliates (ExternalId, PubId, SETID)VALUES('{offer_id[1]}',{pid},'{offer_id[2]}')''')
                            # con.commit()
                cprint(
                    'one pharse done, wait for 1 minutes for next scedule...', 'yellow')
                logging.info(
                    'one pharse done, wait for 1 minutes for next scedule...')
                time.sleep(60)
        except Exception as ex:
            logging.warning('something went to wrong...', ex)
            cprint('something went to wrong...', ex, 'red')


class Task2(Thread):
    def run(self):
        try:
            logging.basicConfig(filename='Logs/affiliate.log', level=logging.INFO,
                                format='%(levelname)s: %(asctime)s : %(message)s', datefmt='%d-%m-%Y : %H:%M:%S')
            print('Process Starting Please Wait ...')
            logging.info('Process Stating Please Wait ...')
            while True:
                for setid in set_ids_task2:
                    con = pyodbc.connect(
                        'DRIVER={SQL Server};SERVER=(local);DATABASE=mobtions;UID=sa;PWD=deepak')

                    query = con.cursor()
                    query.execute(
                        f"select * from OfferLog where AdvertiserSetId='{setid}' and [Status]=1")
                    sql_data = query.fetchall()
                    offer_found = len(sql_data)

                    logging.info(
                        f"Total Active OfferId's Found : {offer_found} : for SetId : {setid}")
                    cprint(
                        f"Total Active OfferId's Found : {offer_found} : for SetId : {setid}", 'green')
                    # pids = [2]

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
                                'API-Key': '446jh787b5711005545j4j4kvj546b465456kj54655k80990cvbc89ffg8e6c7d3d3'
                            }
                            response = requests.request(
                                "POST", url, headers=headers, data=payload)
                            # data = json.loads(response.text.encode('utf8'))
                            logging.info(
                                f'Affiliate was enabled OfferId : {offer_id[0]} SetId:{offer_id[2]} : and pid {pid}')

                            print(
                                f'info : Affiliate was enabled OfferId : {offer_id[0]} SetId : {offer_id[2]} : and pid {pid}')
                            # if offer_id[0] != None:
                            #     pass
                            # query.execute(
                            #     f'''INSERT INTO EnableAffiliates (ExternalId, PubId, SETID)VALUES('{offer_id[1]}',{pid},'{offer_id[2]}')''')
                            # con.commit()
                cprint(
                    'one pharse done, wait for 1 minutes for next scedule...', 'yellow')
                logging.info(
                    'one pharse done, wait for 1 minutes for next scedule...')
                time.sleep(60)
        except Exception as ex:
            logging.warning('something went to wrong...', ex)
            cprint('something went to wrong...', ex, 'red')


t1 = Task1()
t2 = Task2()

t1.start()
t2.start()
