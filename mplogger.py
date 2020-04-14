from multiprocessing import Process, Event, Queue
import logging.handlers, logging.config
from datetime import datetime

sender_config = {
    'version': 1,
    'disable_existing_loggers': True,
    'handlers': {
        'queue': {
            'class': 'logging.handlers.QueueHandler',
            'queue': Queue,
        },
    },
    'loggers': {
        'application': {
            'level':       'DEBUG',
        },
        'proxymanager': {
            'level':       'INFO',
        },
        'tcpconnection': {
            'level':       'WARNING',
        },
        'httpget': {
            'level':       'DEBUG',
        },
        'AthleteHistory': {
            'level':       'INFO',
        },
        'EventResult': {
            'level':       'INFO',
        },
        'EventHistory': {
            'level':       'INFO',
        },
        'dbconnection': {
            'level':       'INFO',
        },
        'test': {
            'level':       'INFO',
        },
        'parkrundb': {
            'level':       'INFO',
        },
    },
    'root': {
        'level': 'INFO',
        'handlers': ['queue']
    },
}

listener_config = {
    'version': 1,
    'disable_existing_loggers': True,
    'respect_handler_level': True,
    'formatters': {
        'detailed': {
            'class':       'logging.Formatter',
            'format':      '%(asctime)-16s:%(name)-21s:%(processName)-15s:%(levelname)-8s[%(module)-16s.%(funcName)-20s %(lineno)-5s] %(message)s'
            },
        'brief': {
            'class':       'logging.Formatter',
            'format':      '%(asctime)-16s:%(message)s'
        }
    },
    'handlers': {
        'console': {
            'class':       'logging.StreamHandler',
            'level':       'CRITICAL',
            'formatter':   'brief'
        },
        'file': {
            'class':       'logging.FileHandler',
            'filename':    (datetime.now().strftime('RUN-%Y%m%d')+'.log'),
            'mode':        'a',
            'formatter':   'detailed',
        },
        #'filerotate': {
        #    'class':       'logging.handlers.TimedRotatingFileHandler',
        #    'filename':    'run.log',
        #    'when':        'midnight',
        #    'interval':    1,
        #    'formatter':   'detailed',
        #    'backupCount': 10
        #}
    },
    'root': {
        'handlers':    ['console', 'file'],
    },
}

#TRACE_LEVEL_NUM = 5 
#logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")
#def trace(self, message, *args, **kws):
#    # Yes, logger takes its '*args' as 'args'.
#    self._log(TRACE_LEVEL_NUM, message, args, **kws) 
#logging.Logger.trace = trace

class MyHandler(object):
    def handle(self, record):
        logger = logging.getLogger(record.name)
        logger.handle(record)

class LogListener(Process):
    def __init__(self, logQueue):
        super(LogListener, self).__init__()
        self.__stop_event = Event()
        self.name = 'listener'
        self.logQueue = logQueue

    def run(self):
        logging.config.dictConfig(listener_config)
        listener = logging.handlers.QueueListener(self.logQueue, MyHandler())
        listener.start()
        while True:
            try:
                self.__stop_event.wait()
                listener.stop()
                break
            except (KeyboardInterrupt, SystemExit):
                listener.stop()
                break

    def stop(self):
        self.__stop_event.set()
