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
client = paho.mqtt.client.Client("Vallls", False)

def on_connect(client, userdata, flags, rc):
	print('conectado publicador')

def Sensor():
    sensor_entrada = pandas.read_sql_query("SELECT  Min(sensor_entrada.idsensor), Max(sensor_entrada.idsensor) From sensor_entrada",con)
    ini= int(pandas.unique(sensor_entrada['min']))
    fin = int(pandas.unique(sensor_entrada['max']))
    return [ini,fin]

def readJson(url, data, data2):
    ram = np.random.randint(1,1000)    
    with open(url) as myfile:
        data=myfile.read()
        json_array = json.loads(data)
        store_list = []
        for item in json_array:
                 store_details = {data:None}
                 store_details[data2] = item[data2]
                 store_list.append(store_details)  
        return store_list[ram][data2]

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

    return (str(numeros[0]) + str(numeros[1]) + ':' + str(numeros[2]) + str(numeros[3]) + ':' + aLetras[0] + aLetras[1] + ':' + str(numeros[4]) + aLetras[2] + ':' + str(numeros[5]) + str(numeros[6]) + ':' + str(numeros[7]) + str(numeros[8]))     

def personasMA():
    edad = np.random.randint(12,90)
    ma = np.random.randint(1,11)
    if edad >= 12 and edad <= 35 and ma <= 8 or edad >= 36 and edad <= 50 and ma <= 7 or edad >= 51 and edad <= 65 and ma <= 5 or edad >= 66 and edad <= 75 and ma <= 3 or edad >= 76 and ma <= 2:
       return [True,edad]
    elif edad >= 12 and edad <= 35 and ma >= 8 or edad >= 36 and edad <= 50 and ma >= 7 or edad >= 51 and edad <= 65 and ma >= 5 or edad >= 66 and edad <= 75 and ma >= 3 or edad >= 76 and ma >= 2:
       return [False,edad]   

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

def sensor_mesa(tienda):
    sensor_mesa = pandas.read_sql_query("Select min(sensor_mesa.idsensor), max(sensor_mesa.idsensor) From sensor_mesa INNER JOIN mesa on sensor_mesa.idmesa = mesa.id INNER JOIN tienda on mesa.idtienda = tienda.id WHERE tienda.id = %s"%tienda,con)
    ini= int(pandas.unique(sensor_mesa['min']))
    fin = int(pandas.unique(sensor_mesa['max']))
    return [ini,fin]

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

def personasCMA(mcAdress,sexo,sensor,hora,mc):
    mcadress = mcAdress
    lastname = readJson('apellidos.json',"last_name",'last_name')
    telefono = np.random.randint(1111111,9999999)
    if sexo == 'female':
        name = readJson('mujeres.json',"first_nameF",'first_nameF')
    else:
        name = readJson('hombres.json',"first_nameM",'first_nameM')
    payload = {
        "idsensor": int(sensor),
        "fecha_hora": str(hora),
        "mcadress": str(mcAdress),
        "nombre": str(name),
        "apellido": str(lastname),
        "telefono": str(telefono),
        "sexo": str(sexo),
        "edad": int(mc[1])
    }
    client.publish('sambil/sensores/entrada',json.dumps(payload),qos=0)
    print(payload)

def compraComida(comidaC,mcAdress,hora,sexo,edad):
    factura = np.random.randint(1111111,9999999)
    monto = np.random.uniform(10,300)
    mesa = np.random.randint(1,3)
    print('mesa:', mesa)
    payload = {
        "factura": int(factura),
        "idtienda": int(comidaC),
        "monto": float(monto),
        "mcadress": str(mcAdress)
    }
    client.publish('sambil/compra/mcadress',json.dumps(payload),qos=0)
    print(payload)
    if mesa == 1 and mcAdress != '':
        hora = hora + datetime.timedelta(minutes= np.random.randint(2,5))
        datosSM = sensor_mesa(comidaC)
        mesa = np.random.randint(datosSM[0],datosSM[0]+1)
        payload = {
        "idsensor": int(mesa),
        "fecha_hora": str(hora),
        "mcadress": str(mcAdress),
        }
        client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
        print(payload)
        time.sleep(0.5)
        hora = hora + datetime.timedelta(minutes= np.random.randint(60,110))
    elif mesa == 1 and mcAdress == '':
        hora = hora + datetime.timedelta(minutes= np.random.randint(2,5))
        datosSM = sensor_mesa(comidaC)
        mesa = np.random.randint(datosSM[0],datosSM[0]+1)
        payload = {
        "idsensor": int(mesa),
        "fecha_hora": str(hora),
        "mcadress": '',
        "nombre": '',
        "apellido": '',
        "telefono": '',
        "sexo": str(sexo),
        "edad": int(edad)
        }
        client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
        print(payload)
        time.sleep(0.5)
        hora = hora + datetime.timedelta(minutes= np.random.randint(60,110))
    if mcAdress != '':
        salirTiendasComidamc(comidaC,hora,mcAdress)
    else:
        salirTiendasComidaNMC(comidaC,hora,sexo,edad)

def salirTiendasComidamc(comidaC,hora,mcAdress):
    datosSensorT = sensor_tiendas(comidaC)
    sensorC = np.random.randint(datosSensorT[0],datosSensorT[1]+1)
    salida = np.random.randint(5,10)
    hora = hora + datetime.timedelta(minutes=salida)
    payload = {
        "idsensor": int(sensorC),
        "fecha_hora": str(hora),
        "mcadress": str(mcAdress)
    }
    client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
    print(payload)
    time.sleep(0.5)

def salirTiendasComidaNMC(comidaC,hora,sexo,edad):
    datosSensorT = sensor_tiendas(comidaC)
    sensorC = np.random.randint(datosSensorT[0],datosSensorT[1]+1)
    salida = np.random.randint(5,10)
    hora = hora + datetime.timedelta(minutes=salida)
    payload = {
        "idsensor": int(sensorC),
        "fecha_hora": str(hora),
        "mcadress": '',
        "nombre": '',
        "apellido": '',
        "telefono": '',
        "sexo": str(sexo),
        "edad": int(edad)
    }
    client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
    print(payload)
    time.sleep(0.5)

def comprarTienda(normal,mcAdress):
    factura = np.random.randint(1111111,9999999)
    monto = np.random.uniform(50,1000)
    payload = {
        "factura": int(factura),
        "idtienda": int(normal),
        "monto": float(monto),
        "mcadress": str(mcAdress)
    }
    client.publish('sambil/compra/mcadress',json.dumps(payload),qos=0)
    print(payload)

def recorrido(hora,mcadress):
    visitas =  cantTiendas(hora)
    print(visitas)
    mcAdress = mcadress
    cond = (visitas[0] + visitas[1] + visitas[2])
    while(cond != 0):
        if visitas[1]==0 and visitas[0] != 0 or visitas[2] == 0 and visitas[0] != 0: #Tiendas Normales
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
            client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
            print(payload)
            time.sleep(0.5)
            comprar = np.random.randint(1,3)
            print('comprar:',comprar)
            if comprar == 1:
                comprarTienda(normal,mcAdress)
            sensorN = np.random.randint(datosNormal[0],datosNormal[1]+1)
            salida = np.random.randint(10,31)
            hora = hora + datetime.timedelta(minutes=salida)
            payload = {
                "idsensor": int(sensorN),
                "fecha_hora": str(hora),
                "mcadress": str(mcAdress)
            }
            client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
            print(payload)
            visitas[0] = visitas[0] - 1
            time.sleep(0.5)
        elif visitas[0] == 0 and visitas[1] != 0 or visitas[2] == 0 and visitas[1] != 0: #Tiendas de comida donde SI compra
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
            client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
            print(payload)
            compraComida(comidaC,mcAdress,hora,'','')
            visitas[1] = visitas[1] - 1
        elif visitas[0] == 0 and visitas[2] != 0 or visitas[1] == 0 and visitas[2] != 0: #Tiendas de comida donde NO compra
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
            client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
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
            client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
            print(payload)
            visitas[2] = visitas[2] - 1
            time.sleep(0.5)
        cond = cond -1
    hora = hora + datetime.timedelta(minutes= np.random.randint(2,8))
    datosSensor= Sensor()
    sensor= np.random.randint(datosSensor[0],datosSensor[1]+1)
    payload = {
                "idsensor": int(sensor),
                "fecha_hora": str(hora),
                "mcadress": str(mcAdress),
                "nombre": '',
                "edad": ''
    }
    client.publish('sambil/sensores/entrada',json.dumps(payload),qos=0)
    print(payload)

def recorridoNMC(hora,sexo,edad):
    visitas =  cantTiendas(hora)
    print(visitas)
    cond = (visitas[0] + visitas[1] + visitas[2])
    while(cond != 0):
        if visitas[1]==0 and visitas[0] != 0 or visitas[2] == 0 and visitas[0] != 0: #Tiendas Normales
            datosTiendas = tiendas()
            normal = np.random.randint(datosTiendas[0],datosTiendas[1]+1)
            datosNormal = sensor_tiendas(normal)
            sensorN = np.random.randint(datosNormal[0],datosNormal[1]+1)
            hora = hora + datetime.timedelta(minutes= np.random.randint(2,8))
            payload = {
                "idsensor": int(sensorN),
                "fecha_hora": str(hora),
                "mcadress": '',
                "nombre": '',
                "apellido": '',
                "telefono": '',
                "sexo": str(sexo),
                "edad": int(edad)
            }
            client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
            print(payload)
            time.sleep(0.5)
            comprar = np.random.randint(1,3)
            print('comprar:',comprar)
            if comprar == 1:
                comprarTienda(normal,'')
            sensorN = np.random.randint(datosNormal[0],datosNormal[1]+1)
            salida = np.random.randint(10,31)
            hora = hora + datetime.timedelta(minutes=salida)
            payload = {
                "idsensor": int(sensorN),
                "fecha_hora": str(hora),
                "mcadress": '',
                "nombre": '',
                "apellido": '',
                "telefono": '',
                "sexo": str(sexo),
                "edad": int(edad)
            }
            client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
            print(payload)
            visitas[0] = visitas[0] - 1
            time.sleep(0.5)
        elif visitas[0] == 0 and visitas[1] != 0 or visitas[2] == 0 and visitas[1] != 0: #Tiendas de comida donde SI compra
            datosTC = tiendasComida()
            comidaC = np.random.randint(datosTC[0],datosTC[1]+1)
            datosSensorT = sensor_tiendas(comidaC)
            sensorC = np.random.randint(datosSensorT[0],datosSensorT[1]+1)
            hora = hora + datetime.timedelta(minutes= np.random.randint(2,8))
            payload = {
                "idsensor": int(sensorC),
                "fecha_hora": str(hora),
                "mcadress": '',
                "nombre": '',
                "apellido": '',
                "telefono": '',
                "sexo": str(sexo),
                "edad": int(edad)
            }
            client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
            print(payload)
            compraComida(comidaC,'',hora,sexo,edad)
            visitas[1] = visitas[1] - 1
        elif visitas[0] == 0 and visitas[2] != 0 or visitas[1] == 0 and visitas[2] != 0: #Tiendas de comida donde NO compra
            datosTNC = tiendasComida()
            comidaNC = np.random.randint(datosTNC[0],datosTNC[1]+1)
            datosSTNC = sensor_tiendas(comidaNC)
            sensorNC = np.random.randint(datosSTNC[0],datosSTNC[1]+1)
            hora = hora + datetime.timedelta(minutes= np.random.randint(2,8))
            payload = {
                "idsensor": int(sensorNC),
                "fecha_hora": str(hora),
                "mcadress": '',
                "nombre": '',
                "apellido": '',
                "telefono": '',
                "sexo": str(sexo),
                "edad": int(edad)
            }
            client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
            print(payload)
            time.sleep(0.5)
            sensorNC = np.random.randint(datosSTNC[0],datosSTNC[1]+1)
            salida = np.random.randint(5,10)
            hora = hora + datetime.timedelta(minutes=salida)
            payload = {
                "idsensor": int(sensorNC),
                "fecha_hora": str(hora),
                "mcadress": '',
                "nombre": '',
                "apellido": '',
                "telefono": '',
                "sexo": str(sexo),
                "edad": int(edad)
            }
            client.publish('sambil/sensores/tiendas-mesas',json.dumps(payload),qos=0)
            print(payload)
            visitas[2] = visitas[2] - 1
            time.sleep(0.5)
        cond = cond -1
    hora = hora + datetime.timedelta(minutes= np.random.randint(2,8))
    datosSensor= Sensor()
    sensor= np.random.randint(datosSensor[0],datosSensor[1]+1)
    payload = {
                "idsensor": int(sensor),
                "fecha_hora": str(hora),
                "mcadress": '',
                "nombre": '',
                "apellido": '',
                "telefono": '',
                "sexo": str(sexo),
                "edad": int(edad)
    }
    client.publish('sambil/sensores/entrada',json.dumps(payload),qos=0)
    print(payload)  
 
def duracionNMC(sensor,hora,sexo,edad):
    payload = {
        "idsensor": int(sensor),
        "fecha_hora": str(hora),
        "mcadress": '',
        "nombre": '',
        "apellido": '',
        "telefono": '',
        "sexo": str(sexo),
        "edad": int(edad)
    }
    client.publish('sambil/sensores/entrada',json.dumps(payload),qos=0)
    print(payload)
    time.sleep(0.5)
    recorridoNMC(hora,sexo,edad)

def devolver_ma():
    cur = con.cursor()
    cur.execute("SELECT mcadress.mcadress FROM mcadress")
    rows = cur.fetchall()
    ma_usuarios = []
    for row in rows:
        ma_usuarios.append(row)
    return ma_usuarios

def escoger_ma(ma):
    ma_array = ma
    ma_escogido = np.random.randint(0,len(ma_array))
    return ma_array[ma_escogido]

def ultimoAcceso(mcadress):
    ultimoAcceso = pandas.read_sql_query("select fecha_hora from sensor_mcadress where mcadress = '%s' order by fecha_hora desc limit 1"%mcadress,con)
    fecha = str(pandas.unique(ultimoAcceso['fecha_hora']))
    return fecha
    

def main():
    client.qos = 0
    client.connect(host='localhost')
    meanEntrada = 10 
    stdEntrada = 2
    day = 1
    while(day != 30):
        personasPorDia = np.random.normal(meanEntrada, stdEntrada)
        while(personasPorDia>1):
            print('persona nueva')
            horaBase= datetime.datetime.now().replace(hour=0,minute=0, second=0, day=day)
            hora = horaBase + datetime.timedelta(hours=np.random.uniform(10,20)) + datetime.timedelta(minutes=np.random.uniform(0,60))
            datosSensor= Sensor()
            sensor= np.random.randint(datosSensor[0],datosSensor[1]+1)
            ma_r = np.random.normal(1,11)
            if ma_r >=1 and ma_r <=7:
                sexo = Gender()
                mc = personasMA()
                if  mc[0] == True:
                    mcAdress =  generateMC()
                    personasCMA(mcAdress,sexo,sensor,hora,mc)
                    time.sleep(0.5)
                    recorrido(hora,mcAdress)

                else:
                    duracionNMC(sensor,hora,sexo,mc[1])
            else:
                print('entro!!')
                sexo = Gender()
                mc = personasMA()
                ma_dev = devolver_ma()
                if ma_dev == []:
                    print('pero entro aqui :(')
                    mc = personasMA()
                    if  mc[0] == True:
                        mcAdress =  generateMC()
                        personasCMA(mcAdress,sexo,sensor,hora,mc)
                        time.sleep(0.5)
                        recorrido(hora,mcAdress)
                    else:
                        duracionNMC(sensor,hora,sexo,mc[1])
                else:    
                    ma_escogido = escoger_ma(ma_dev)[0]
                    ultimoA = ultimoAcceso(ma_escogido)
                    separar = ultimoA.split('-',3)
                    mes = int(separar[1])
                    dia = int(separar[2].split(' ')[0])
                    print(dia,hora.day,mes,hora.month)
                    if dia == hora.day and mes == hora.month:
                        print('pero entro aqui :( 2')
                        mc = personasMA()
                        if  mc[0] == True:
                            mcAdress =  generateMC()
                            personasCMA(mcAdress,sexo,sensor,hora,mc)
                            time.sleep(0.5)
                            recorrido(hora,mcAdress)
                        else:
                            duracionNMC(sensor,hora,sexo,mc[1])
                    else:     
                        payload = {
                            "idsensor": int(sensor),
                            "fecha_hora": str(hora),
                            "mcadress": str(ma_escogido),
                            "nombre": '',
                            "edad": ''
                        }
                        client.publish('sambil/sensores/entrada',json.dumps(payload),qos=0)
                        print(payload)
                        time.sleep(0.5)
                        recorrido(hora,ma_escogido)  
            personasPorDia-=1
        day = day +1




if __name__ == '__main__':
	main()
	sys.exit(0)