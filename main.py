"""
Google App Engine service to record sensors' hearbeat

The corresponding arduino code will :
   - get the GMT time and initialize its local clock
   - update its heartbeat with the value of millis()
   - read back its value to know when was the last time it was updated

It's possible also from the dashboard to get the last time a sensor sent 
its hearbeat

TODO : GAE DB keys need to be reserved to avoid collisions
TODO : Error detection when inserting new hearbeat

"""


from framework import bottle
from framework.bottle import run, request, response, Bottle
from time import mktime, gmtime
from datetime import datetime
import json
from google.appengine.ext import db


APIKEY = "abc1de"
app = Bottle()   

def valid_key(request):
    if  request.headers.get("X-ApiKey") == APIKEY:
        return True
    else:
        return False
        
@app.get('/')
def status():
    """
    Get GAE GMT time - used like a poor man NTP 
    return epoch time
    """
    response.content_type = "text/plain"
    d = long(mktime(gmtime()))
    return "%s"%d

@app.get('/sensors/<id>')
def sensorGET(id):
    """
    Get sensor's last seen hearbeat
    """
    response.content_type = "text/plain"
    k = db.Key.from_path('Sensor', '%s'%id)
    s = db.get(k)
    if s is None:
        response.status = 404
        return('Sensor not found')
    return "%s"%s.lastSeen
    
@app.put('/sensors/<id>')
def sensorPUT(id):
    """
    Put one sensor hearbeats
    """
    
    response.content_type = "text/plain"
    
    if not valid_key(request):
        response.status = 403
        return('Invalid Key')
        
    s = Sensor(key_name='%s'%id)
    
    d = request.body.read()
    if not d:
        response.status = 400
        return('No Data')
        
    s.lastSeen = long(d)
    s.timestamp = datetime.utcnow()
    k = s.put()
    return "OK"

@app.post('/sensors/')
def sensorPOST():
    """
    Post multiple sensor hearbeats (unlikely to have multiple)
    in the form :
    {"1":1010102, "2":120202}
    
    """
    response.content_type = "text/plain"
    
    if not valid_key(request):
        response.status = 403
        return('Invalid Key')
        
    d = request.body.read()
    if not d:
        response.status = 400
        return('No Data')
        
    try:
        payload = json.loads(d)
    except Exception, e:
        response.status = 400
        return('Invalid Format')
        
    for k,v in payload.iteritems():
        s = Sensor(key_name=k)
        s.lastSeen = long(v)
        s.timestamp = datetime.utcnow()
        result = s.put()
        
    return "OK"

class Sensor(db.Model):
    # epoch time of the last update
    lastSeen = db.IntegerProperty()
    timestamp = db.DateTimeProperty()


bottle.run(app=app, server='gae')
