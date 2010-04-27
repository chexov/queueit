#!/usr/bin/env python
import sys
import beanstalkc

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print "Usage %s bt://host:port <queue> <object id> [<object id>, <object id>]" % (sys.argv[0] )
        sys.exit(1)
    
    url = sys.argv[1]
    queue_name = sys.argv[2]
    if url.find('bt://') > -1:
        hostport_str = url.split('://')[1]
        host, port = hostport_str.split(':')
        port = int(port)
        try:
            queue_conn = beanstalkc.Connection(host=host, port=port)
        except beanstalkc.SocketError:
            print "Can't connect to %s:%s" % (host, port)
            sys.exit(1)
    else:
        print "Unsupported scheme %s" % (url.split('://')[0] )
        sys.exit(1)

    
    for object_id in sys.argv[2:]:
        queue_conn.use(queue_name)
        taskid = queue_conn.put(str(object_id) )
        print "OK: %s; %s; %s" % (object_id, queue_name, taskid)

