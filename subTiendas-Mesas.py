import ssl
import sys
import paho.mqtt.client
import psycopg2
import pandas
import numpy
import matplotlib
import json

conn = psycopg2.connect(host = 'localhost', user= 'postgres', password ='27450917', dbname= 'proyecto1')


def sensor_mcadress(a):
    cur = conn.cursor()
    sql = '''INSERT INTO sensor_mcadress (mcadress,idsensor,fecha_hora) VALUES ( %s, %s, %s);'''
    cur.execute(sql, (a["mcadress"],a["idsensor"],a["fecha_hora"]))
    conn.commit()

def sensor_usuario(a):
    cur = conn.cursor()
    sql = '''INSERT INTO sensor_usuario (idsensor,fecha_hora,edad,sexo) VALUES ( %s, %s, %s, %s);'''
    cur.execute(sql, (a["idsensor"],a["fecha_hora"],a["edad"],a["sexo"]))
    conn.commit()

def on_connect(client, userdata, flags, rc):    
    print('conectado (%s)' % client._client_id)
    client.subscribe(topic='sambil/#', qos = 0)        

def on_message(client, userdata, message):   
    a = json.loads(message.payload)
    print(a)
    print('------------------------------')
    if a['mcadress'] != '':
        sensor_mcadress(a)
    else:
        sensor_usuario(a)
    

def main():	
	client = paho.mqtt.client.Client()
	client.on_connect = on_connect
	client.message_callback_add('sambil/sensores/tiendas-mesas', on_message)
	client.connect(host='localhost')
	client.loop_forever()


if __name__ == '__main__':
	main()
	sys.exit(0)