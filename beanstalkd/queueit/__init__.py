#!/usr/bin/env python
# encoding: utf-8
import sys
import os
import logging
import time

import beanstalkc

from beanstalkc import * # TODO: remove it

logging.basicConfig(level=logging.DEBUG, format=u'%(asctime)-15s %(name)-12s: %(levelname)-8s %(message)s')
LOG = logging.getLogger('queueit')
LOG.setLevel(logging.DEBUG)


# Loading values from the shell ENV
QHOST = os.environ.get('QUEUEIT_HOST', '127.0.0.1')
QPORT = 11300
QTIMEOUT = None
QPRIORITY = os.environ.get('QUEUEIT_PRIORITY', beanstalkc.DEFAULT_PRIORITY)

try:
    QTTR = int(os.environ.get('QUEUEIT_TTR', 120))
except ValueError:
    LOG.error("Incorrect value for QUEUEIT_TTR. Using '%s' instead" % QTTR)

try:
    QPRIORITY = int(os.environ.get('QUEUEIT_PRIORITY', beanstalkc.DEFAULT_PRIORITY))
except ValueError:
    LOG.error("Incorrect value for QUEUEIT_PRIORITY. Using '%s' instead" % QPRIORITY)

try:
    QPORT = int(os.environ.get('QUEUEIT_PORT', 11300))
except ValueError:
    LOG.error("Incorrect value for QUEUEIT_PORT. Using '%s' instead" % QPORT)

try:
    QTIMEOUT = os.environ.get('QUEUEIT_TIMEOUT', QTIMEOUT)
    if QTIMEOUT:
        QTIMEOUT = int(QTIMEOUT)
except ValueError:
    LOG.error("Incorrect value for QTIMEOUT. Using '%s' instead" % QTIMEOUT)


def _get_qconnection(host, port):
    try:
        return Connection(host=host, port=port)
    except SocketError:
        print "Can't connect to %s:%s" % (host, port)
        sys.exit(1)


def qget(tube_name, qconn=None):
    """
    Reserving job from the tube, printing the job.body and deleting that job.
    Be aware that job is already gone from the beanstalkd standpoint
    and operator must deal with it.
    """
    if not qconn:
        qconn = _get_qconnection(QHOST, QPORT)

    qconn.watch(tube_name)
    job = qconn.reserve()
    print job.body
    job.delete()
    qconn.close()


def qput(tube_name, messages, qconn=None):
    if not qconn:
        qconn = _get_qconnection(QHOST, QPORT)

    for message in messages:
        qconn.use(tube_name)
        jobid = qconn.put(str(message), ttr=QTTR, priority=QPRIORITY)
        print "OK: message=%s; tube=%s; job.id=%s; ttr=%s; priority=%s" % (message, tube_name, jobid, QTTR, QPRIORITY)
    qconn.close()


def qkick(tube_name, count=1, qconn=None):
    if not qconn:
        qconn = _get_qconnection(QHOST, QPORT)
    qconn.use(tube_name)
    print qconn.kick(count)


def qstat(qconn=None, delay=None):
    def compare_tubes(last, cur):
        _cur = cur.copy()
        for _k, _v in last.items():
            try:
                change = int(_cur[_k]) - int(_v)
                if change > 0:
                    change = "+%s" % change
                elif change == 0:
                    change = ""
                _cur[_k] = "%-5s %s" % (_cur[_k], change)
            except ValueError:
                pass
                # Not an number
        return _cur

    def compare_numbers(last, cur):
        _cur = cur
        change = int(_cur) - int(last)
        if change > -1:
            change = "+%s" % change
        _cur = "%s %s" % (_cur, change)
        return _cur

    if not qconn:
        qconn = _get_qconnection(QHOST, QPORT)
    tubes_stats_last = {}
    while True:
        LINE="%-24s %-10s %-10s %-10s %-10s %-10s"
        print LINE % ('tube', 'watching', 'buried', 'ready', 'delayed', 'reserved')
        tubes = qconn.tubes()
        tubes.sort()
        for tube in tubes:
            if tube:
                name = str(tube)
                tube_stats_cur = qconn.stats_tube(tube)
                if tubes_stats_last.get(name):
                    tube_stats = compare_tubes(tubes_stats_last[name], tube_stats_cur)
                else:
                    tube_stats = tube_stats_cur

                #tube_stats = qconn.stats_tube(tube)
                print LINE % (name,
                        tube_stats.get('current-watching'),
                        tube_stats.get('current-jobs-buried'),
                        tube_stats.get('current-jobs-ready'),
                        tube_stats.get('current-jobs-delayed'),
                        tube_stats.get('current-jobs-reserved'))
                tubes_stats_last[name] = tube_stats_cur
        if delay:
            time.sleep(delay)
        else:
            return None


def qwrapperbatch(tube_in, tube_out, worker_cmd, batch_size=10):
    import subprocess
    import os
    _env = os.environ
    qconn_in  = _get_qconnection(QHOST, QPORT)
    qconn_out = _get_qconnection(QHOST, QPORT)
    qconn_in.watch(tube_in)
    qconn_out.use(tube_out)
    while True:
        batch_jobs = []
        cmd = []
        cmd.extend(worker_cmd)

        # Filling the pool
        while len(batch_jobs) < batch_size:
            LOG.debug(u"Filling up the job pool: %s jobs already. %s needed" % (len(batch_jobs), batch_size))
            try:
                job = qconn_in.reserve(timeout=QTIMEOUT)
                if not job:
                    LOG.info(u"Timeout reached")
                batch_jobs.append(job)
            except DeadlineSoon:
                LOG.debug(u"Oops. Got DeadlineSoon. Not waiting for queue to fill up..")
                #LOG.debug(u"Oops. Got DeadlineSoon. Touching reserved jobs..")
                map(lambda j: j.touch(), batch_jobs)
                break
                #continue

        cmd.extend(map(lambda j: j.body, batch_jobs))

        LOG.debug(u"Calling command %s...%s" % (u" ".join(cmd)[:30], u" ".join(cmd)[-15:]))
        #LOG.debug(u"Calling command %s" % (u" ".join(cmd)))
        retcode = subprocess.call(cmd)

        if not retcode == 0:
            def _bury(job):
                job.bury()
                LOG.info(u"Job %s buried" % job.jid)
            map(_bury, batch_jobs)
        else:
            def _put(job):
                jid = qconn_out.put(job.body)
                print job.body
                job.delete()
                LOG.info(u"New job put in to %s: %s" % (tube_out, jid))
            map(_put, batch_jobs)


def qcleanup(qname):
    qconn  = _get_qconnection(QHOST, QPORT)
    print "Cleaninig up", qconn.use(qname)
    while True:
        job = qconn.peek_buried()
        if not job:
            break
        print "Deleting:", job.jid, job.body
        job.delete()


# FIXME: this is copypaste from local draft. not working. should be fixed
def qwrapperwithstats():
        stattistics_queue_name = sys.argv[1]
        obj_id = sys.argv[2]
        command = u" ".join(sys.argv[3:])

        QUEUESTATS_CONN = get_qconn_or_die(HOST,PORT)
        QUEUESTATS_CONN.use(stattistics_queue_name)

        log_id = "%s.%s" % (socket.gethostname(), uuid4().hex)

        log_redirect = " > %s/%s.log 2>&1" % (LOG_DIR, log_id)
        cmd = u"".join([command, log_redirect])

        LOG.debug(u"Calling command %s" % cmd)
        time_started = int(time.time())
        retcode = subprocess.call(cmd, shell=True)
        time_duration = int(time.time()) - time_started
        LOG.info(u"Exit code is %s" % retcode)


        task_stats = {'obj_id': obj_id,
                      'duration': time_duration,
                      'started': time_started,
                      'worker': command,
                      'ecode': retcode,
                      'log_id': log_id }
        QUEUESTATS_CONN.put(json.dumps(task_stats))
        LOG.info(u"Task statistics: %s" % json.dumps(task_stats))
        sys.exit(retcode)


def qwrapper(tube_in, tube_out, worker_cmd):
    import subprocess
    qconn_in  = _get_qconnection(QHOST, QPORT)
    qconn_out = _get_qconnection(QHOST, QPORT)
    qconn_in.watch(tube_in)
    qconn_out.use(tube_out)

    LOG.info(u"QHOST: %s, QPORT %s, TTR %s, PRIORITY %s, Watching queues: %s" % (QHOST, QPORT, QTTR, QPRIORITY, qconn_in.watching()) )
    while True:
        job = qconn_in.reserve(timeout=QTIMEOUT)
        if not job:
            LOG.info(u"Timeout reached. Bye-bye")
            sys.exit()
        params = job.body

        # If we have {} inside the command, replace this with job payload. like xargs(1)
        if u" ".join(worker_cmd).find('{}') > -1:
            cmd = map(lambda arg: arg.replace('{}', job.body), worker_cmd)
        else:
            cmd = list(worker_cmd)
            cmd.append(job.body)

        LOG.info(u"Got job {0}".format(job.stats()))
        LOG.info(u"Calling command '%s'" % cmd)
        retcode = subprocess.call(cmd)
        if not retcode == 0:
            LOG.error(u"Worker command '%s' was exited with retcode %s" % (cmd, retcode) )
            job.bury()
            LOG.info(u"Job %s buried" % job.jid)
        else:
            if tube_out != "null":
                jid = qconn_out.put(str(params), ttr=QTTR, priority=QPRIORITY)
                LOG.info(u"New job put in to %s: %s" % (tube_out, jid))
            job.delete()


def qpeeknext(qname, peek_type):
    '''
    peeks at the next job in a tube
    '''
    qconn  = _get_qconnection(QHOST, QPORT)
    print "Peeking at next %s job in queue" % peek_type, qconn.use(qname)
    job = getattr(qconn, 'peek_%s' % peek_type)()
    if not job:
        print 'No %s jobs in %s' % (peek_type, qname)
        return
    print job.jid, job.body


def qpeekjob(jid):
    '''
    peeks at the next job in a tube
    '''
    qconn  = _get_qconnection(QHOST, QPORT)
    print "Peeking at job_id %s" % jid
    job = qconn.peek(int(jid))
    if not job:
        print 'Job %s not found' % jid
        return
    print job.jid, job.body


def main():
    try:
        COMMAND = os.path.basename(sys.argv[0])
        args = sys.argv[1:]
        if COMMAND == 'queueit':
            if len(sys.argv) == 1:
                print "Usage:"
                print "%s q-get" % COMMAND
                print "%s q-put" % COMMAND
                print "%s q-kick" % COMMAND
                print "%s q-stat" % COMMAND
                print "%s q-wrapper" % COMMAND
                print "%s q-wrapper-batch" % COMMAND
                print "%s q-wrapper-with-stats" % COMMAND
                print "%s q-peek" % COMMAND
                print "%s q-peek-ready" % COMMAND
                print "%s q-peek-delayed" % COMMAND
                print "%s q-peek-buried" % COMMAND
                sys.exit(1)
            else:
                COMMAND = os.path.basename(sys.argv[1])
                args = sys.argv[2:]


        if COMMAND == 'q-get':
            if not len(args) == 1:
                print "Usage: %s <queue>" % (COMMAND)
                sys.exit(1)
            qget(args[0])
        elif COMMAND == 'q-put':
            if len(args) == 1:
                qput(args[0], [sys.stdin.read(),])
            elif len(args) > 1:
                qput(args[0], args[1:])
            else:
                print "Usage: %s <queue> [<message>, <message>, ...]\n Message body could be sent trough STDIN" % (COMMAND )
                sys.exit(1)
        elif COMMAND == 'q-kick':
            if len(args) < 1 or len(args) > 2:
                print "Usage: %s <queue> [<count>]" % COMMAND
                sys.exit(1)

            count = 1
            if len(args) == 2:
                try:
                    count = int(args[1])
                except ValueError:
                    print "Wrong count value '%s'. Using default %s" % (args[1], count)
            qkick(args[0], count)
        elif COMMAND == 'q-stat':
            if len(args) == 1:
                qstat(delay=int(args[0]))
            else:
                qstat()
        elif COMMAND == 'q-wrapper':
            if len(args) >= 3:
                qwrapper(args[0], args[1], worker_cmd=args[2:])
            else:
                print "Usage: %s <queue-in> <queue-out> [<cmd>]\n <cmd> could be sent trough STDIN" % (COMMAND)
                print sys.exit(1)
        elif COMMAND == 'q-wrapper-with-stats':
            if len(args) == 4:
                qwrapperwithstats(stats_queue_name, job_id, command)
            else:
                print "Usage: %s <statistics-queue> <job_id> <cmd>" % (COMMAND)
                print sys.exit(1)
        elif COMMAND == 'q-wrapper-batch':
            if len(args) >= 4:
                qwrapperbatch(args[0], args[1], worker_cmd=args[3:], batch_size=int(args[2]))
            else:
                print "Usage: %s <queue-in> <queue-out> <batch-size> <cmd>" % (COMMAND)
                print sys.exit(1)
        elif COMMAND == 'q-cleanup':
            if len(args) == 1:
                qcleanup(args[0])
            else:
                print "Usage: %s <queue>" % (COMMAND)
                print sys.exit(1)
        elif COMMAND == 'q-peek':
            if len(args) == 1:
                qpeekjob(args[0])
            else:
                print "Usage: %s <job_id>" % (COMMAND)
                print sys.exit(1)
        elif COMMAND == 'q-peek-ready':
            if len(args) == 1:
                qpeeknext(args[0], peek_type="ready")
            else:
                print "Usage: %s <queue>" % (COMMAND)
                print sys.exit(1)
        elif COMMAND == 'q-peek-delayed':
            if len(args) == 1:
                qpeeknext(args[0], peek_type="delayed")
            else:
                print "Usage: %s <queue>" % (COMMAND)
                print sys.exit(1)
        elif COMMAND == 'q-peek-buried':
            if len(args) == 1:
                qpeeknext(args[0], peek_type="buried")
            else:
                print "Usage: %s <queue>" % (COMMAND)
                print sys.exit(1)

        else:
            print "Unknown command '%s'" % COMMAND
    except KeyboardInterrupt:
        print "Keyboard Interrupt. Bye-bye"


if __name__ == "__main__":
    main()
