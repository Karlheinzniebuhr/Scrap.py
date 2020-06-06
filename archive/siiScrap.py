# Made with love by Karlpy


'''
El dominio ya parece estar offline
'''


import requests
import pymongo
from pymongo import MongoClient
from lxml import etree
from io import StringIO

siis_url = 'https://sistema.siis.gov.py/fuentes_externas/legajo_participante.php?p_documento='

# Cedula (id) range
start = 1
stop = 1000

cedula_generator = (i for i in range(start, stop + 1))

client = MongoClient()
db = client.siis
siisdb = db.siis_data

session = requests.Session()
parser = etree.HTMLParser()

for ced in cedula_generator:
    try:
        req_url = f'{siis_url}{ced}'
        req = session.get(req_url)
        html = req.content.decode("utf-8")
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
            siisdb.insert_one(persona_dict)
            # print(persona_dict)

    except Exception as e:
        print(e)
        continue