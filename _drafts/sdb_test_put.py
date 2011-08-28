import boto.sdb.connection
from boto.exception import SDBResponseError
import settings
import random
import datetime
import time
import threading

def main():
    conn = boto.sdb.connection.SDBConnection(settings.AWS_ACCESS_KEY, settings.AWS_SECRET_KEY)
    domname = 'test-speed'
    try:
        domain = conn.get_domain(domname)
    except SDBResponseError, e:
        domain = conn.create_domain(domname)
        time.sleep(1)
    
    id = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    
    data = {'v1': random.randint(0,1000000)}
    domain.put_attributes(id, data)
    
    data = {'v2': random.randint(0,1000000)}
    domain.put_attributes(id, data)
    
    data = {'v2': random.randint(0,1000000)}
    domain.put_attributes(id, data)
    
    # expected: v1 & v2 co-exist and are scalar; fail on v2-write removes v1-write.
    item = domain.get_attributes(id, consistent_read=True)
    print(repr(item))

    # result: as expected:
    #$ python sdb_test_put.py 
    #{u'v1': u'287576', u'v2': u'579136'}


if __name__ == '__main__':
    main()
