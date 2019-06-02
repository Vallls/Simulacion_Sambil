import ssl
import sys
import paho.mqtt.client
import psycopg2
import pandas
import matplotlib
import simplejson as json
import random
import time
import paho.mqtt.publish
import numpy as np
import datetime


con = psycopg2.connect(host = 'localhost', user= 'postgres', password ='27450917', dbname= 'proyecto1')

def on_connect(client, userdata, flags, rc):
	print('conectado publicador')

def readJson(url,data,data2):
    with open(url) as myfile:
        data=myfile.read()
        json_array = json.loads(data)
        store_list = []
        for item in json_array:
                 store_details = {data:None}
                 store_details[data2] = item[data2]
                 store_list.append(store_details)  
        print(store_list[0][data2])

def Gender():
    genderRan = np.random.randint(1,3)
    if genderRan == 1:
        return "female"
    else:
        return "male"

def generateMC():
    cond = 0
    i = 0
    numeros = []
    letras = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']
    aLetras = []
    while(cond < 9):
        numero = np.random.randint(0,10)
        numeros.append(numero) 
        i = i + 1
        cond = cond + 1
    cond = 0
    i = 0
    while(cond < 3):
        letra = letras[np.random.randint(0,26)]
        aLetras.append(letra)  
        i = i + 1
        cond = cond + 1

    return str(numeros[0]) + str(numeros[1]) + ':' + str(numeros[2]) + str(numeros[3]) + ':' + aLetras[0] + aLetras[1] + ':' + str(numeros[4]) + aLetras[2] + ':' + str(numeros[5]) + str(numeros[6]) + ':' + str(numeros[7]) + str(numeros[8])

def calculoHora(hora,tardar):
    a = hora.hour
    b = hora.minute
    falta = 60 - b
    if falta >= tardar:
        return [a,b+tardar]
    elif falta < tardar:
        if tardar % 60 == 0:
            return[a + (tardar/60),b]
        else:
            resto = tardar - falta
            a = a + 1
            if resto < 60:
                return [a,b+resto]
            else:
                while(resto > 60):
                    a = a + 1
                    resto = resto - 60  
                return [a,resto]

def cantTiendas(hora):
    if hora.hour >= 10 and hora.hour <= 12:
       normales = np.random.randint(0,8)
       comidaC =  np.random.randint(0,3)
       comidaNC = np.random.randint(0,4)
    elif hora.hour >= 13 and hora.hour <= 15:
       normales = np.random.randint(0,5)
       comidaC =  np.random.randint(0,2)
       comidaNC = np.random.randint(0,3)
    elif hora.hour >= 16 and hora.hour <= 18:
       normales = np.random.randint(0,4)
       comidaC =  np.random.randint(0,2)
       comidaNC = np.random.randint(0,3)
    elif hora.hour >= 19 and hora.hour <= 20:
       normales = np.random.randint(0,2)
       comidaC = 0
       comidaNC = 0
    return [normales,comidaC,comidaNC]

def tiendas():
    tienda = pandas.read_sql_query("Select min(tienda.id), max(tienda.id) from tienda left join mesa on tienda.id = mesa.idtienda where mesa.idtienda is null",con)
    ini= int(pandas.unique(tienda['min']))
    fin = int(pandas.unique(tienda['max']))
    return [ini,fin]

def tiendasComida():
    comida = pandas.read_sql_query("Select min(mesa.idtienda), max(mesa.idtienda) from mesa",con)
    ini= int(pandas.unique(comida['min']))
    fin = int(pandas.unique(comida['max']))
    return [ini,fin]

def sensor_tiendas(tienda):
    sensor_entrada = pandas.read_sql_query("SELECT  Min(sensor_tienda.idsensor), Max(sensor_tienda.idsensor) From sensor_tienda where sensor_tienda.idtienda = %s"%tienda,con)
    ini= int(pandas.unique(sensor_entrada['min']))
    fin = int(pandas.unique(sensor_entrada['max']))
    return [ini,fin]

def recorrido(hora,mcadress,client):
    visitas =  cantTiendas(hora)
    print(visitas)
    mcAdress = mcadress
    cond = (visitas[0] + visitas[1] + visitas[2])
    print(cond)
    while(cond != 0):
        ran = np.random.randint(1,4)
        if ran == 1 and visitas[0] != 0 or visitas[1]==0 and visitas[0] != 0 or visitas[2] == 0 and visitas[0] != 0:
            datosTiendas = tiendas()
            normal = np.random.randint(datosTiendas[0],datosTiendas[1]+1)
            datosNormal = sensor_tiendas(normal)
            sensorN = np.random.randint(datosNormal[0],datosNormal[1]+1)
            hora = hora + datetime.timedelta(minutes= np.random.randint(2,8))
            payload = {
                "idsensor": int(sensorN),
                "fecha_hora": str(hora),
                "mcadress": str(mcAdress)
            }
            client.publish('sambil/sensores/tiendas',json.dumps(payload),qos=0)
            print(payload)
            time.sleep(0.5)
            sensorN = np.random.randint(datosNormal[0],datosNormal[1]+1)
            salida = np.random.randint(10,31)
            hora = hora + datetime.timedelta(minutes=salida)
            payload = {
                "idsensor": int(sensorN),
                "fecha_hora": str(hora),
                "mcadress": str(mcAdress)
            }
            client.publish('sambil/sensores/tiendas',json.dumps(payload),qos=0)
            print(payload)
            time.sleep(0.5)
        elif ran == 2 and visitas[1] != 0 or visitas[0] == 0 and visitas[1] != 0 or visitas[2] == 0 and visitas[1] != 0:
            datosTC = tiendasComida()
            comidaC = np.random.randint(datosTC[0],datosTC[1]+1)
            datosSensorT = sensor_tiendas(comidaC)
            sensorC = np.random.randint(datosSensorT[0],datosSensorT[1]+1)
            hora = hora + datetime.timedelta(minutes= np.random.randint(2,8))
            payload = {
                "idsensor": int(sensorC),
                "fecha_hora": str(hora),
                "mcadress": str(mcAdress)
            }
            client.publish('sambil/sensores/tiendas',json.dumps(payload),qos=0)
            print(payload)
            time.sleep(0.5)
            sensorC = np.random.randint(datosSensorT[0],datosSensorT[1]+1)
            salida = np.random.randint(60,120)
            hora = hora + datetime.timedelta(minutes=salida)
            payload = {
                "idsensor": int(sensorC),
                "fecha_hora": str(hora),
                "mcadress": str(mcAdress)
            }
            client.publish('sambil/sensores/tiendas',json.dumps(payload),qos=0)
            print(payload)
            time.sleep(0.5)
        elif ran == 3 and visitas[2] != 0 or visitas[0] == 0 and visitas[2] != 0 or visitas[1] == 0 and visitas[2] != 0:
            datosTNC = tiendasComida()
            comidaNC = np.random.randint(datosTNC[0],datosTNC[1]+1)
            datosSTNC = sensor_tiendas(comidaNC)
            sensorNC = np.random.randint(datosSTNC[0],datosSTNC[1]+1)
            hora = hora + datetime.timedelta(minutes= np.random.randint(2,8))
            payload = {
                "idsensor": int(sensorNC),
                "fecha_hora": str(hora),
                "mcadress": str(mcAdress)
            }
            client.publish('sambil/sensores/tiendas',json.dumps(payload),qos=0)
            print(payload)
            time.sleep(0.5)
            sensorNC = np.random.randint(datosSTNC[0],datosSTNC[1]+1)
            salida = np.random.randint(5,10)
            hora = hora + datetime.timedelta(minutes=salida)
            payload = {
                "idsensor": int(sensorNC),
                "fecha_hora": str(hora),
                "mcadress": str(mcAdress)
            }
            client.publish('sambil/sensores/tiendas',json.dumps(payload),qos=0)
            print(payload)
            time.sleep(0.5)
        
        cond = cond -1

def devolver_ma():
    cur = con.cursor()
    cur.execute("SELECT mcadress.mcadress FROM mcadress")
    rows = cur.fetchall()
    ma_usuarios = []
    for row in rows:
        ma_usuarios.append(row)
    return ma_usuarios

def main():
    hola = devolver_ma()
    if hola == []:
        print('holis')

if __name__ == '__main__':
	main()
	sys.exit(0)