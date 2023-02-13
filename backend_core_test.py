import eventlet
import json
from flask import Flask, render_template,request
from flask_mqtt import Mqtt
from flask_socketio import SocketIO, emit
from flask_bootstrap import Bootstrap
import sqlite3
import datetime
from threading import Lock
import sqlite3


eventlet.monkey_patch()

app = Flask(__name__)
app.config['SECRET'] = 'my secret key'
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['MQTT_BROKER_URL'] = 'broker.hivemq.com'
app.config['MQTT_BROKER_PORT'] = 1883
app.config['MQTT_USERNAME'] = ''
app.config['MQTT_PASSWORD'] = ''
app.config['MQTT_KEEPALIVE'] = 5
app.config['MQTT_TLS_ENABLED'] = False
app.config['MQTT_CLEAN_SESSION'] = True

# Parameters for SSL enabled
# app.config['MQTT_BROKER_PORT'] = 8883
# app.config['MQTT_TLS_ENABLED'] = True
# app.config['MQTT_TLS_INSECURE'] = True
# app.config['MQTT_TLS_CA_CERTS'] = 'ca.crt'

mqtt = Mqtt(app)
mqtt2 = Mqtt(app)
mqtt3 = Mqtt(app)
async_mode = None
socketio = SocketIO(app)


bootstrap = Bootstrap(app)

#Database


data =""
#temperature measurement table

#fix this down
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

conn = sqlite3.connect(':memory:')
conn.row_factory = dict_factory
##conn = sqlite3.connect('fastoryDB.db')

c = conn.cursor()

c.execute("""CREATE TABLE IF NOT EXISTS temperaturelist (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             nID int,
             temperature int,
             time TIMESTAMP,
             units text
             );""")

c.execute("""CREATE TABLE IF NOT EXISTS heartbeatlist (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             nID int,
             heartbeat text,
             time TIMESTAMP
             );""")

c.execute("""CREATE TABLE IF NOT EXISTS event (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             nID int,
             ThresholdTriggered bool,
             thresholdLevel text,
             time TIMESTAMP
             );""")

#create a robot
def insert_temperature(nID, temperature, time, units):
    with conn:
        c.execute("INSERT INTO temperaturelist VALUES (:id, :nID, :temperature, :time,:units )", {'id':None, 'nID': nID,'temperature': temperature, 'time': time, 'units': units})

#create an event
def insert_heartbeat(nID, heartbeat, time):
    with conn:
        c.execute("INSERT INTO heartbeatlist VALUES (:id, :nID, :heartbeat, :time)", {'id':None, 'nID': nID,'heartbeat': heartbeat, 'time':time})

def insert_event(nID, ThresholdTriggered, thresholdLevel, time):
    with conn:
        c.execute("INSERT INTO event VALUES (:id,:nID, :ThresholdTriggered, :thresholdLevel, :time)", {'id':None, 'nID': nID,'ThresholdTriggered': ThresholdTriggered, 'thresholdLevel':thresholdLevel, 'time': datetime.datetime.now()})

def get_all_temps():
    #c.execute("SELECT * FROM temperaturelist WHERE temperature=:temperature", {'temperature': temperature})
    sqlSt="SELECT * FROM temperaturelist WHERE 1"
    c.execute(sqlSt)
    return c.fetchall()

def get_all_heart():
    sqlSt = "SELECT * FROM heartbeatlist WHERE 1"
    c.execute(sqlSt)
    return c.fetchall()

def get_all_alarm():
    sqlSt = "SELECT * FROM event WHERE 1"
    c.execute(sqlSt)
    return c.fetchall()
##fix this up,

@app.route('/<string:page_name>/')
def static_page(page_name):
    nID = request.args.get('nID')
    return render_template('%s.html' % page_name,nID=nID)

@app.route('/nodes/<id>/temperature-measurements/latest', methods=['GET'])
def getState(id):
    print ("temperature")
    now = datetime.datetime.now()
    time= now.isoformat();
    cnvMsg = data
    cnvMsg_str = json.dumps(cnvMsg)
    return cnvMsg_str

@app.route('/nodes/<id>/temperature-measurements', methods=['GET'])
def getState1(id):
    print ("temperature list")
    cnvMsg = get_all_temps()
    cnvMsg_str = json.dumps(cnvMsg)
    return cnvMsg_str

@app.route('/nodes/<id>/heartbeat/latest', methods=['GET'])
def getState2(id):
    print ("heartbeat")
    now = datetime.datetime.now()
    time= now.isoformat();

    cnvMsg = dataheart
    cnvMsg_str = json.dumps(cnvMsg)
    return cnvMsg_str

@app.route('/nodes/<id>/heartbeat', methods=['GET'])
def getState3(id):
    print ("heartbeat list")
    now = datetime.datetime.now()
    time= now.isoformat();

    cnvMsg = get_all_heart()
    cnvMsg_str = json.dumps(cnvMsg)
    return cnvMsg_str

@app.route('/nodes/<id>/events/latest', methods=['GET'])
def getState4(id):
    print ("event")
    now = datetime.datetime.now()
    time= now.isoformat();

    cnvMsg = data3
    cnvMsg_str = json.dumps(cnvMsg)
    return cnvMsg_str

@app.route('/nodes/<id>/events', methods=['GET'])
def getState5(id):
    print ("event list")
    now = datetime.datetime.now()
    time= now.isoformat();

    cnvMsg = get_all_alarm()
    cnvMsg_str = json.dumps(cnvMsg)
    return cnvMsg_str

@socketio.on('publish')
def handle_publish(json_str):
    data = json.loads(json_str)
    mqtt.publish(data['topic'], data['message'])


@socketio.on('subscribe')
def handle_subscribe(json_str):
    data = json.loads(json_str)
    print("Subscription requested on topic: ")
    mqtt.subscribe(data['topic'])

@socketio.on('subscribe')
def handle_subscribe(json_str):
    dataheart = json.loads(json_str)
    print("Subscription requested on topic: ")
    mqtt.subscribe(dataheart['topic'])

@socketio.on('unsubscribe_all')
def handle_unsubscribe_all():
    mqtt.unsubscribe_all()

@socketio.event
def my_ping():
    emit('my_ping')
##############################################

@mqtt.on_message()
def handle_mqtt_message(client, userdata, message):
    global data
    data = dict(
        topic=message.topic,
        payload=message.payload.decode()
    )
    plj = message.payload.decode()
    pl = json.loads(plj)
    nID = int(pl["nID"])
    temp = int(pl["temperature"])
    time = pl["time"]
    units = pl["unit"]
    insert_temperature(nID, temp, time, units)
    print(data, "here this is")


@mqtt.on_log()
def handle_logging(client, userdata, level, buf):
    print(level, buf)
    print("all temp")
    alltemps = get_all_temps()
    print(alltemps)

@mqtt.on_connect()
#def handle_connect(client, userdata, flags, rc):
 #   mqtt.subscribe('arduinox/tuantemperature')

###########################################
@mqtt2.on_log()
def handle_logging(client, userdata, level, buf):
    print("all heartbeat")
    allheart = get_all_heart()
    print(allheart)
    print(level, buf)



@mqtt2.on_message()
def handle_mqtt_message(client, userdata, message):
    global dataheart
    dataheart = dict(
        topic=message.topic,
        payload=message.payload.decode()
    )
    pljheart = message.payload.decode()
    plheart = json.loads(pljheart)
    nID = int(plheart["nID"])
    heart = (plheart["heartbeat"])
    time = plheart["time"]
    insert_heartbeat(nID, heart, time)
    print(dataheart, "heartbeat here this is")

#@mqtt2.on_connect()
#def handle_connect(client, userdata, flags, rc):
#    mqtt2.subscribe('arduinox/tuanheartbeat')


########################################

@mqtt3.on_message()
def handle_mqtt_message(client, userdata, message):
    global data3
    data3 = dict(
        topic=message.topic,
        payload=message.payload.decode()
    )
    plj = message.payload.decode()
    pl = json.loads(plj)
    nID = int(pl["nID"])
    alarm = int(pl["ThresholdTriggered"])
    time = pl["time"]
    thresholdLevel = pl["thresholdLevel"]
    insert_event(nID, alarm,thresholdLevel, time)
    print(data3, "alarm here this is")

@mqtt3.on_log()
def handle_logging(client, userdata, level, buf):
    print(level, buf)
    print("all event")
    allalarm = get_all_alarm()
    print(allalarm)

@mqtt3.on_connect()
def handle_connect(client, userdata, flags, rc):
    mqtt.subscribe('arduinox/tuantemperature')
    mqtt2.subscribe('arduinox/tuanheartbeat')
    mqtt3.subscribe('arduinox/tuanalarm')


if __name__ == '__main__':

    # important: Do not use reloader because this will create two Flask instances.
    # Flask-MQTT only supports running with one instance
    socketio.run(app, host='0.0.0.0', port=5000, use_reloader=False, debug=False)