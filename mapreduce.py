#!/bin/env python
# encoding: utf-8

"""
A simple mapreduce without distributed filesystem.

@author: wujiabin
"""

import heapq
import itertools
import json
import logging
import multiprocessing
import os
import re
import time
import traceback

import zerorpc


FORMAT = '%(asctime)s %(threadName)s %(process)d %(lineno)d %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG)

logger = logging.getLogger('mapreduce')


MASTER_SERVER = 'tcp://127.0.0.1:4242'


class MapReduce(object):

    def __init__(self, **kwargs):

        # input 可以是file, 可以是目录
        self._input = kwargs.pop('input')
        # output为目录
        self._output = kwargs.pop('output')
        # 中间结果存放的地方
        self._tmp = kwargs.pop('tmp')

        if not os.path.exists(self._tmp):
            os.makedirs(self._tmp)

        self._map_count = kwargs.pop('map_count')
        self._reduce_count = kwargs.pop('reduce_count')

        self._job = kwargs.pop('job')
        self._job_instance = self._job()

    @property
    def job(self):
        return self._job_instance

    @property
    def input(self):
        return [os.path.join(self._input, i) for i in os.listdir(self._input)]


class Master(object):

    def __init__(self, mr):
        self._mr = mr
        self._workers = set()

        def _wrapper():
            s = zerorpc.Server(self)
            s.bind('tcp://0.0.0.0:4242')
            s.run()

        self._process = multiprocessing.Process(name='master', target=_wrapper)

    def start(self):
        self._process.start()

    def join(self, timeout=None):
        self._process.join(timeout)

    def terminate(self):
        self._process.terminate()

    def run_mr(self):
        # map
        logger.info("Start map...")

        workers = []
        for worker in self._workers:
            w = zerorpc.Client()
            w.connect(worker)
            workers.append(w)

        worker_index = 0
        map_count = min(self._mr._map_count, len(mr.input))
        intermediate_output = []
        # rpc调度是同步的
        for i, input_file in zip(range(map_count), mr.input):
            intermediate_output.append(workers[worker_index].do_map(i, input_file))
            worker_index = (worker_index + 1) % len(workers)

        logger.info("Map is done.")

        logger.info("Start reduce...")

        # reduce
        worker_index = 0
        output = []
        for index, reduce_inputs in enumerate(zip(*intermediate_output)):
            output.append(workers[worker_index].do_reduce(index, reduce_inputs))
            worker_index = (worker_index + 1) % len(workers)
        print output

        logger.info("Reduce is done.")

    def register(self, worker):
        logger.debug("register worker. [worker={w}]".format(w=worker))
        self._workers.add(worker)

    def workers(self):
        return list(self._workers)


class Worker(object):

    def __init__(self, name, port, mr):
        self._name = name
        self._mr = mr
        self._tcp_url = 'tcp://0.0.0.0:%d' % port

        def _wrapper():
            master = zerorpc.Client()
            master.connect(MASTER_SERVER)
            master.register('tcp://127.0.0.1:%d' % port)
            print master.workers()

            s = zerorpc.Server(self)
            s.bind(self._tcp_url)
            s.run()

        self._process = multiprocessing.Process(name='worker_%s' % name, target=_wrapper)

    def start(self):
        self._process.start()

    def join(self, timeout=None):
        self._process.join(timeout)

    def terminate(self):
        self._process.terminate()

    def do_map(self, index, input_file):
        reduce_count = self._mr._reduce_count
        basename = os.path.basename(input_file)
        tmp = self._mr._tmp
        output_files = [os.path.join(tmp, 'map_%d.%s.r%d' % (index, basename, i)) for i in range(reduce_count)]
        output_fds = [open(f, 'w') for f in output_files]
        output_buffers = [[] for _ in output_files]
        with open(input_file) as f:
            for line in f:
                try:
                    for k, v in self._mr.job.map(input_file, line):
                        output_index = hash(k) % reduce_count
                        output_buffers[output_index].append({'key': k, 'value': v})
                except:
                    logger.warning('exception: {}'.format(traceback.format_exc()))
        # 排序
        for index, _buffer in enumerate(output_buffers):
            _buffer.sort(key=lambda i: i['key'])
            for item in _buffer:
                print >> output_fds[index], json.dumps(item)
        [f.close() for f in output_fds]

        return output_files

    def do_reduce(self, index, inputs):

        def _build_iterator(fd):
            for line in fd:
                item = json.loads(line)
                yield item['key'], item['value']

        output = os.path.join(self._mr._output, 'reduce_%d.out' % index)
        input_fds = [open(i) for i in inputs]
        fd_iterators = [_build_iterator(fd) for fd in input_fds]
        iterator = ((key, [i[1] for i in values]) for key, values in itertools.groupby(heapq.merge(*fd_iterators), key=lambda i: i[0]))
        with open(output, 'w') as out:
            for key, values in iterator:
                for v in self._mr.job.reduce(key, values):
                    print >> out, json.dumps({'key': key, 'value': v})
        return output


class WordCount(object):

    def map(self, key, value):
        for word in re.split(r'\W', value):
            if word:
                yield word, 1

    def reduce(self, key, values):
        s = 0
        for i in values:
            s += i
        yield s


if __name__ == '__main__':
    env = {
        'input': './input',
        'output': './output',
        'tmp': './tmp',
        'map_count': 8,
        'reduce_count': 8,
        'job': WordCount,
    }
    mr = MapReduce(**env)

#    import sys
#    role = sys.argv[1]
#
#    if role == 'master':
#        master = Master(mr)
#        master.start()
#    elif role == 'worker':
#        worker = Worker(1, 4343, mr)
#        worker.start()

    master = Master(mr)
    master.start()

    worker_start_port = 4243
    workers = []
    for i in range(4):
        worker = Worker(i, worker_start_port+i, mr)
        worker.start()
        workers.append(worker)

    import time
    time.sleep(1)

    def run():
        master = zerorpc.Client()
        master.connect(MASTER_SERVER)
        master.run_mr()

    run()

    # stop
    master.terminate()
    [worker.terminate() for worker in workers]
