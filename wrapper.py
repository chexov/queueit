#!/usr/bin/env python
# encoding: utf-8
import sys
import logging
import subprocess
import string

import beanstalkc

logging.basicConfig()
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print "Usage %s bt://host:port <storage> <input queue> <output queue> <command>" % (sys.argv[0])
        sys.exit(1)

    URL, STORAGE, QIN_NAME, QOUT_NAME, COMMAND = sys.argv[1:]
    if URL.find('bt://') > -1:
        hostport_str = URL.split('://')[1]
        host, port = hostport_str.split(':')
        port = int(port)
        try:
            QIN = beanstalkc.Connection(host=host, port=port)
        except beanstalkc.SocketError:
            print "Can't connect to %s:%s" % (host, port)
            sys.exit(1)

        try:
            QOUT = beanstalkc.Connection(host=host, port=port)
        except beanstalkc.SocketError:
            print "Can't connect to %s:%s" % (host, port)
            sys.exit(1)
    else:
        print "Unsupported scheme %s" % (url.split('://')[0])
        sys.exit(1)

    QIN.watch(QIN_NAME)
    LOG.info(u"Watching queues: %s" % QIN.watching())

    while True:
        job = QIN.reserve(timeout=60)
        if not job:
            print "Timoeout riched. bye bye"
            QIN.close()
            QOUT.close()
            sys.exit(1)
        job_releases = job.stats()['releases']
        object_id = job.body
        cmd = string.join([COMMAND, STORAGE, object_id])
        LOG.debug(u"Calling command %s" % cmd)
        retcode = subprocess.call(cmd, shell=True)
        if not retcode == 0:
            LOG.error(u"Worker command was exited with retcode %s" % retcode)
            job.release(delay=1 + job_releases * 30)
            LOG.info(u"Job %s released" % job.jid)
        else:
            QOUT.use(QOUT_NAME)
            jid = QOUT.put(str(object_id))
            job.delete()
            LOG.info(u"New job put in to %s: %s" % (QOUT_NAME, jid))

