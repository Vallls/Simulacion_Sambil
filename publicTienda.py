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

def usuarios(table):
    cur = con.cursor()
    cur.execute("SELECT * FROM %s"%table)
    rows = cur.fetchall()
    usuarios = []
    for row in rows:
        usuarios.append(row)
    return usuarios

def sensor_tiendas():
    sensor_entrada = pandas.read_sql_query("SELECT  Min(sensor_tienda.idsensor), Max(sensor_tienda.idsensor) From sensor_tienda",con)
    ini= int(pandas.unique(sensor_entrada['min']))
    fin = int(pandas.unique(sensor_entrada['max']))
    return [ini,fin]

def comprobarTCM(sensor):
    cur = con.cursor()
    cur.execute("SELECT mesa.idtienda From mesa INNER JOIN sensor_tienda on mesa.idtienda = sensor_tienda.idtienda WHERE sensor_tienda.idsensor = %s GROUP BY mesa.idtienda"%sensor)
    idtienda = cur.fetchall()
    if idtienda == []:
        return idtienda
    else:
        return idtienda[0][0]

def main():
    client = paho.mqtt.client.Client("Vallls", False)
    client.qos = 0
    client.connect(host='localhost')
    usuarios_mcAdress = usuarios('sensor_mcadress')
    usuarios_notmcAdress = usuarios('sensor_usuario')
    datosSensor= sensor_tiendas()
    sensor= np.random.randint(datosSensor[0],datosSensor[1]+1)
    print(sensor)
    print(comprobarTCM(sensor))
    

if __name__ == '__main__':
	main()
	sys.exit(0)