"""
Goggle App Engine proxy to COSM

Emulate the COSM API to work around API response time up to 6s
Actual processing of the sensor's request is performed asynchronously
using GAE queues with a 10s timeout.

TODO : support more content type.
TODO : configure COSM API keys through the GAE DB

"""


from framework import bottle
from framework.bottle import request, response, Bottle
from time import mktime, gmtime
from datetime import datetime,timedelta
import logging
import httplib
import json
from google.appengine.ext import db
from google.appengine.api import taskqueue

from twilio.rest import TwilioRestClient

app = Bottle()


class Sensor(db.Model):
    timestamp = db.DateTimeProperty()
    lastValues = db.StringProperty()


class Config(db.Model):
    sid = db.StringProperty()
    token = db.StringProperty()
    from_number = db.StringProperty()
    to_number = db.StringProperty()
    noop = db.BooleanProperty()


class Stream(db.Model):
    frozen = db.BooleanProperty()
    sent = db.BooleanProperty()
    errorcount = db.IntegerProperty()


class cosm(object):
    """
        Interface with COSM
    """

    def __init__(self):
        """Constructor for cosm"""
        super(cosm, self).__init__()

    @staticmethod
    def update_feed(feedid,
                    key,
                    body,
                    contentType = 'text/csv',
                    timeout=5):
        """ Sends records to cosm using update feed"""

        try:
            headers = {'X-ApiKey':key,'Content-Type':contentType}

            h = httplib.HTTPConnection("api.cosm.com", timeout=timeout)
            h.request('PUT','/v2/feeds/%s'%feedid, body=body, headers=headers)
            stat = h.getresponse().status
        except httplib.HTTPException, e:
            stat = 0
        return stat

    @staticmethod
    def create_datapoint(feedid,
                         key,
                         datastream,
                         datapoints,
                         contentType='text/csv',
                         timeout=5):
        """ Sends datapoints to cosm using create datapoints
            time in the body is created using datetime.utcnow().isoformat(),
        """

        try:
            headers = {'X-ApiKey': key ,'Content-Type':contentType}

            h = httplib.HTTPConnection("api.cosm.com", timeout=timeout)
            h.request('PUT',
                '/v2/feeds/%s/datastreams/%s/datapoints'%(feedid,datastream),
                body=datapoints,
                headers=headers)
            stat = h.getresponse().status
        except httplib.HTTPException, e:
            stat = 0
        return stat


    @staticmethod
    def get_sensor(stream, contentType):
        """get the sensor id and measurement tuple """
        if (contentType == 'text/csv'):
            a,b = stream.split(',')
            return stream.split(',')
        else:
            return None

    @staticmethod
    def get_streams(body, contentType):
        """ get the list of streams from the body"""
        if (contentType == 'text/csv'):
            return body.split()
        else:
            return None

    @staticmethod
    def get_key(headers):
        return headers.get("X-ApiKey", None)

@app.get('/')
def status():
    """
    Application Status
    """
    sendSMS('test')
    response.content_type = "text/plain"
    d = long(mktime(gmtime()))
    return "%s" % d

@app.put('/v2/feeds/<feedid>')
def feedPUT(feedid):
    """
    Emulate the cosm API to update a feed
    """

    d = request.body.read()
    if not d:
        response.status = 400
        return('No Data')

    body = {
        'feedid': feedid,
        'body': d,
        'key': cosm.get_key(request.headers),
        'contentType': request.headers.get('Content-Type')
    }

    taskqueue.add(url='/admin/cosm', method='POST', payload=json.dumps(body))

    for stream in cosm.get_streams(body['body'], body['contentType']):
        sensor,value = cosm.get_sensor(stream, body['contentType'])
        #save the timestamp and last values
        s = Sensor(key_name='%s:%s'%(feedid,sensor))
        s.timestamp = datetime.utcnow()
        s.lastValues = value
        s.put()

    response.content_type = "text/plain"
    return 'OK'

@app.post('/admin/cosm')
def cosmPOST():

    payload = request.body.read()
    if not payload:
        response.status = 400
        return('No Data')
    b = json.loads(payload)
    feedid = b['feedid']
    body = b['body']
    key = b['key']
    contentType = b['contentType']

    stat = cosm.update_feed(feedid=feedid,
                            key=key,
                            contentType=contentType,
                            body=body,
                            timeout=10 )

    # if stat is not 200, returning something other than 2xx
    # will cause GAE to retry to send the result
    # for now, just return 200
    response.status = 200
    return 'OK'

@app.get('/admin/heartbeat')
def heartbeat():

    sensors = Sensor.all()
    now = datetime.utcnow()
    delta = timedelta(minutes=10)
    frozen = False
    stream_name = None
    for s in sensors:
        key_name = s.key().name()
        stream_name = key_name.split(':')[0]
        if s.timestamp + delta < now:
            logging.debug('Frozen stream: %s - last seen: %s' % (stream_name,
                                                                 s.timestamp))
            frozen = True
            break
    stream = Stream.get_by_key_name(stream_name)
    if stream is None:
        logging.debug('New stream. Creating %s' % stream_name)
        ns = Stream(key_name=stream_name)
        ns.errorcount = 0 if not frozen else 1
        ns.frozen = frozen
        ns.sent = False
        ns.put()
    else:
        logging.debug('Found stream %s' % stream_name)
        if frozen:
            if stream.errorcount > 3:
                if not stream.sent:
                    sendSMS('Stream %s Frozen' % stream_name)
                    stream.sent = True
            stream.frozen = frozen
            stream.errorcount = int(stream.errorcount) + 1
            stream.put()
        else:
            if stream.frozen:
                # was frozen before
                sendSMS('Stream %s OK : errors %d' % (stream_name,
                                                      int(stream.errorcount)))
                stream.sent = False
                stream.frozen = frozen
                stream.errorcount = 0
                stream.put()
    response.status = 200
    return 'OK'

@app.get('/admin/init')
def init():

    response.status = 200
    return 'OK'


def sendSMS(text):

    noop = None
    try:
        query = Config.all()
        query.run(limit=1)
        c = list(query)[0]
        account_sid = c.sid
        auth_token = c.token
        from_number = c.from_number
        to_number = c.to_number
        noop = c.noop
    except:
        logging.error('Cannot retrieve config')
    if not noop:
        client = TwilioRestClient(account_sid, auth_token)
        logging.debug('Sending SMS')
        rv = client.sms.messages.create(to=to_number,
                                        from_=from_number,
                                        body=text)

    return 'OK'

def main():

    bottle.run(app=app, server='gae')

if __name__ == "__main__":
    main()
