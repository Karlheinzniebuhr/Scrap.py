# Made with love by Karlpy
# Update: Ya no funciona el scrapper porque ahora se puso un Captcha en el webservice

import requests, csv, json, asyncio, time
from lxml import etree, html
from aiohttp import ClientSession
ips_url = 'http://servicios.ips.gov.py/consulta_asegurado/comprobacion_de_derecho_externo.php'

# Cedula (id) range
start = 1
stop = 100

param_generator = ({'nro_cic':str(c), 'elegir':'', 'envio':'ok','recuperar':'Recuperar'} for c in range(start, stop))

async def aiohttp_post_to_page(data, session):
    async with session.post(ips_url,data=data) as response:
        return await response.text()

async def fetch_data(sem, param, session):
    async with sem:
        start = time.time()
        result_html = await aiohttp_post_to_page(param, session)
        ced = param['nro_cic']
        t = (time.time() - start)
        return t, ced, result_html

async def main():   
    # done, pending = await asyncio.wait(futures, timeout=5)
    with open ('100k.csv','w',newline='',encoding='utf-8') as csvfile:
        hp = etree.HTMLParser(encoding='utf-8')
        writer=csv.writer(csvfile)
        writer.writerow(['nro_documento', 'nombres', 'apellidos', 'fecha_nacim', 'sexo', 'tipo_aseg', 'beneficiarios_activos', 'enrolado','vencimiento_de_fe_de_vida','nro_titular', 'titular', 'estado_titular', 'meses_de_aporte_titular','vencimiento_titular','ultimo_periodo_abonado_titular'])
        start_time = time.time()
        async with ClientSession() as session:
            sem = asyncio.Semaphore(100)
            futures = [asyncio.ensure_future(fetch_data(sem, param, session)) for param in param_generator]
            for i, future in enumerate(asyncio.as_completed(futures)):
                #print(future.result())
                try:
                    t, ced, result_html = await future
                    root = html.fromstring(result_html, parser=hp)
                    nro_documento = root.xpath(u"/html/body/center[2]/form/table[2]/tr[2]/td[2]")[0].text.strip()
                    nombres = root.xpath(u"/html/body/center[2]/form/table[2]/tr[2]/td[3]")[0].text.strip()
                    apellidos = root.xpath(u"/html/body/center[2]/form/table[2]/tr[2]/td[4]")[0].text.strip()
                    fecha_nacim = root.xpath(u"/html/body/center[2]/form/table[2]/tr[2]/td[5]")[0].text.strip()
                    sexo = root.xpath(u"/html/body/center[2]/form/table[2]/tr[2]/td[6]")[0].text.strip()
                    tipo_aseg = root.xpath(u"/html/body/center[2]/form/table[2]/tr[2]/td[7]")[0].text.strip()
                    beneficiarios_activos = root.xpath(u"/html/body/center[2]/form/table[2]/tr[2]/td[8]")[0].text.strip()
                    enrolado = root.xpath(u"/html/body/center[2]/form/table[2]/tr[2]/td[9]")[0].text.strip()
                    vencimiento_de_fe_de_vida = root.xpath(u"/html/body/center[2]/form/table[2]/tr[2]/td[10]")[0].text.strip()

                    nro_titular = root.xpath(u"/html/body/center[2]/form/table[3]/tr[2]/td[1]")[0].text.strip()
                    titular = root.xpath(u"/html/body/center[2]/form/table[3]/tr[2]/td[2]")[0].text.strip()
                    estado_titular = root.xpath(u"/html/body/center[2]/form/table[3]/tr[2]/td[3]")[0].text.strip()
                    meses_de_aporte_titular = root.xpath(u"/html/body/center[2]/form/table[3]/tr[2]/td[4]")[0].text.strip()
                    vencimiento_titular = root.xpath(u"/html/body/center[2]/form/table[3]/tr[2]/td[5]")[0].text.strip()
                    ultimo_periodo_abonado_titular = root.xpath(u"/html/body/center[2]/form/table[3]/tr[2]/td[6]")[0].text.strip()

                    print('{}, {}, {} retornado en {:.2f} segundos'.format(nro_documento, nombres, apellidos, t))
                    writer.writerow([nro_documento, nombres, apellidos, fecha_nacim, sexo, tipo_aseg, beneficiarios_activos, enrolado, vencimiento_de_fe_de_vida, nro_titular, titular, estado_titular, meses_de_aporte_titular, vencimiento_titular, ultimo_periodo_abonado_titular])

                except Exception as e:
                    print("Cedula: %s no existe" %(ced))
                    continue
            t_total = time.time() - start_time
            nr_of_requests = ((stop+1) - start)
            print("Process took: {:.2f} seconds".format(t_total))
            print("{} requests per second".format(nr_of_requests/t_total))

asyncio.run(main())