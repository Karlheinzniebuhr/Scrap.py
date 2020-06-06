# Made with love by Karlpy

'''
El dominio ya parece estar offline
'''

import requests
import pymongo
from pymongo import MongoClient
from lxml import etree
from io import StringIO
import asyncio
from aiohttp import ClientSession

# event loop error fix: https://github.com/encode/httpx/issues/914
import sys
if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

siis_url = 'https://sistema.siis.gov.py/fuentes_externas/legajo_participante.php?p_documento='

# Cedula (id) range
start = 1
stop = 1000

url_generator = (f'{siis_url}{ced}' for ced in range(start, stop + 1))

client = MongoClient()
db = client.siis
siisdb = db.siis_data3

session = requests.Session()
parser = etree.HTMLParser()

async def fetch(url, session):
    async with session.get(url) as response:
        html = await response.text()
        return html

async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        return await fetch(url, session)

async def main():
    async with ClientSession() as session:
        sem = asyncio.Semaphore(30)
        futures = [asyncio.ensure_future(bound_fetch(sem, url, session)) for url in url_generator]
        print(len(futures))
        for future in asyncio.as_completed(futures):
            try:
                html = await future
                if not html:
                    continue

                tree = etree.parse(StringIO(html), parser=parser)

                persona_dict = {}
                persona_dict['apellido'] = tree.find('//*[@id="e_apellido"]').attrib['value'].strip()
                persona_dict['nombre'] = tree.find('//*[@id="e_nombre"]').attrib['value'].strip()
                persona_dict['nro_documento'] = tree.find('//*[@id="e_documento"]').attrib['value'].strip()
                persona_dict['sexo'] = tree.find('//*[@id="e_sexo"]').attrib['value'].strip()
                persona_dict['nacionalidad'] = tree.find('//*[@id="e_nacionalidad"]').attrib['value'].strip()
                persona_dict['lugar_de_nacimiento'] = tree.find('//*[@id="e_lugar"]').attrib['value'].strip()
                persona_dict['fecha_nacimiento'] = tree.find('//*[@id="e_fec_nac"]').attrib['value'].strip()
                persona_dict['edad'] = tree.find('//*[@id="e_edad"]').attrib['value'].strip()

                if (persona_dict['apellido'] != None) and (persona_dict['apellido'] != ''):
                    print(f'{persona_dict["nro_documento"]} {persona_dict["nombre"]} {persona_dict["apellido"]}')
                    # siisdb.insert_one(persona_dict)
                    # print(persona_dict)

            except Exception as e:
                print(e)
                continue

asyncio.run(main())