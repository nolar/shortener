import boto.sdb.connection
from boto.exception import SDBResponseError
import settings
import random
import datetime
import time
import threading

def main2(thid, domsuffix=''):
    conn = boto.sdb.connection.SDBConnection(settings.AWS_ACCESS_KEY, settings.AWS_SECRET_KEY)
    domname = 'test-speed'+domsuffix
    try:
        domain = conn.get_domain(domname)
    except SDBResponseError, e:
        domain = conn.create_domain(domname)
        time.sleep(1)
    
    n = 1000
    prefix = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S') + '-' + thid
    
    ts = time.time()
    for i in xrange(n):
        id = prefix + '-' + str(i)
        data = {'id': id, 'prefix':prefix, 'value': random.randint(0,1000000)}
        domain.put_attributes(id, data)
    te = time.time()
    td = te-ts
    print('thid=%s, dom=%s, te=%f pr put, of %f puts per sec' % (thid, domname, td/n, n/td))

def main():
    for i in xrange(10):
        thread = threading.Thread(target=main2, args=(str(i),str(i),))
        thread.start()

if __name__ == '__main__':
    main()
