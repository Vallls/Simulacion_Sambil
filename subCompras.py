import ssl
import sys
import paho.mqtt.client
import psycopg2
import pandas
import numpy
import matplotlib
import json

conn = psycopg2.connect(host = 'localhost', user= 'postgres', password ='27450917', dbname= 'proyecto1')


def compra(a):
    cur = conn.cursor()
    sql = '''INSERT INTO compra (factura,idtienda,monto) VALUES ( %s, %s, %s);'''
    cur.execute(sql, (a["factura"],a["idtienda"],a["monto"]))
    conn.commit()

def mcadress_compra(a):
    cur = conn.cursor()
    sql = '''INSERT INTO mcadress_compra (factura,mcadress) VALUES ( %s, %s, %s);'''
    cur.execute(sql, (a["factura"],a["mcadress"], a["cedula"]))
    conn.commit()

def on_connect(client, userdata, flags, rc):    
    print('conectado (%s)' % client._client_id)
    client.subscribe(topic='sambil/#', qos = 0)        

def on_message(client, userdata, message):   
    a = json.loads(message.payload)
    print(a)
    print('------------------------------')
    compra(a)
    if a["mcadress"] != '':
        mcadress_compra(a)
    

def main():	
	client = paho.mqtt.client.Client()
	client.on_connect = on_connect
	client.message_callback_add('sambil/compra/mcadress', on_message)
	client.connect(host='localhost')
	client.loop_forever()


if __name__ == '__main__':
	main()
	sys.exit(0)