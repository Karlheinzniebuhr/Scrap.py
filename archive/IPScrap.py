# Made with love by Karlpy
# Update: Ya no funciona el scrapper porque ahora se puso un Captcha en el webservice

import requests, csv, json
from lxml import etree, html

ips_url = 'http://servicios.ips.gov.py/consulta_asegurado/comprobacion_de_derecho_externo.php'

# Cedula (id) range
start = 3000260
stop = 3000270

param_dict_list = []

for c in range(start, stop):
    param_dict_list.append({'nro_cic':str(c), 'elegir':'', 'envio':'ok','recuperar':'Recuperar'})

with open ('datos_ips_sync.csv','w',newline='',encoding='utf-8') as csvfile:
    writer=csv.writer(csvfile)
    writer.writerow(['nro_documento', 'nombres', 'apellidos', 'fecha_nacim', 'sexo', 'tipo_aseg', 'beneficiarios_activos', 'enrolado','vencimiento_de_fe_de_vida','nro_titular', 'titular', 'estado_titular', 'meses_de_aporte_titular','vencimiento_titular','ultimo_periodo_abonado_titular'])

    session = requests.Session()
    for ced in param_dict_list:
        try:
            cedula = ced['nro_cic']
            r = session.post(ips_url, data = ced)
            root = html.fromstring(r.text)
            nro_documento = root.xpath('/html/body/center[2]/form/table[2]/tr[2]/td[2]')[0].text.strip()
            nombres = root.xpath('/html/body/center[2]/form/table[2]/tr[2]/td[3]')[0].text.strip()
            apellidos = root.xpath('/html/body/center[2]/form/table[2]/tr[2]/td[4]')[0].text.strip()
            fecha_nacim = root.xpath('/html/body/center[2]/form/table[2]/tr[2]/td[5]')[0].text.strip()
            sexo = root.xpath('/html/body/center[2]/form/table[2]/tr[2]/td[6]')[0].text.strip()
            tipo_aseg = root.xpath('/html/body/center[2]/form/table[2]/tr[2]/td[7]')[0].text.strip()
            beneficiarios_activos = root.xpath('/html/body/center[2]/form/table[2]/tr[2]/td[8]')[0].text.strip()
            enrolado = root.xpath('/html/body/center[2]/form/table[2]/tr[2]/td[9]')[0].text.strip()
            vencimiento_de_fe_de_vida = root.xpath('/html/body/center[2]/form/table[2]/tr[2]/td[10]')[0].text.strip()

            nro_titular = root.xpath('/html/body/center[2]/form/table[3]/tr[2]/td[1]')[0].text.strip()
            titular = root.xpath('/html/body/center[2]/form/table[3]/tr[2]/td[2]')[0].text.strip()
            estado_titular = root.xpath('/html/body/center[2]/form/table[3]/tr[2]/td[3]')[0].text.strip()
            meses_de_aporte_titular = root.xpath('/html/body/center[2]/form/table[3]/tr[2]/td[4]')[0].text.strip()
            vencimiento_titular = root.xpath('/html/body/center[2]/form/table[3]/tr[2]/td[5]')[0].text.strip()
            ultimo_periodo_abonado_titular = root.xpath('/html/body/center[2]/form/table[3]/tr[2]/td[6]')[0].text.strip()

            print(nro_documento, nombres, apellidos)
            writer.writerow([nro_documento, nombres, apellidos, fecha_nacim, sexo, tipo_aseg, beneficiarios_activos, enrolado, vencimiento_de_fe_de_vida, nro_titular, titular, estado_titular, meses_de_aporte_titular, vencimiento_titular, ultimo_periodo_abonado_titular])

        except Exception as e:
            print("Cedula %s no existe" %(cedula))
            continue