#!/usr/bin/env python

import sys
import os
import re
import random
import threading
import subprocess
import numpy as np
import pyqtgraph as pg
from pyqtgraph.Qt import QtGui, QtCore
from pprint import pprint

# Our API
import oltpbench

# Enable antialiasing for prettier plots
pg.setConfigOptions(antialias=True)
#pg.setConfigOptions(useOpenGL=True)

#QtGui.QApplication.setGraphicsSystem('raster')
app = QtGui.QApplication([])
#mw = QtGui.QMainWindow(parent=None, flags=QtCore.Qt.FramelessWindowHint)
#mw.resize(800,800)

GRAPH_INTERVAL = 100 # ms
BENCHMARK_INTERVAL = 1000 # ms
FINISHED = False
START_INTERVAL = 300

PLOT_WIDTH = 1600
PLOT_HEIGHT = 200

p = None
PLOT_DATA = [0]*START_INTERVAL
PLOT_LOCATION = 0
PLOT_CURVE = None
PLOT_THROUGHPUT = None
PLOT_THROUGHPUT_AMOUNT = None
PLOT_LINE_POSITIONS = set()
PLOT_LINES = { }
PLOT_LINE_WIDTH = 5
NEW_DATA = [ ]
NUM_INDEXES = 0
EXPECTED_INDEXES = 10

INDEX_MAP = {
    "CUSTOMER_PKEY": ["CUSTOMER",  "c_w_id", "c_d_id", "c_id"],
    "IDX_CUSTOMER_NAME": ["CUSTOMER",  "c_w_id", "c_d_id", "c_last"],
    "DISTRICT_PKEY": ["DISTRICT", "d_w_id", "d_id"],
    "ITEM_PKEY": ["ITEM", "i_id"],
    "NEW_ORDER_PKEY": ["NEW_ORDER", "no_w_id", "no_d_id", "no_o_id"],
    "IDX_ORDER": ["OORDER", "o_w_id", "o_d_id", "o_c_id", "o_id"],
    "OORDER_PKEY": ["OORDER", "o_w_id", "o_d_id", "o_id"],
    "ORDER_LINE_PKEY": ["ORDER_LINE", "ol_w_id", "ol_d_id", "ol_o_id", "ol_number"],
    "STOCK_PKEY": ["STOCK", "s_w_id", "s_i_id"],
    "WAREHOUSE_PKEY": ["WAREHOUSE", "w_id"],
}

NOTIFICATION_SOUND = "/home/pavlo/Dropbox/optimized.wav"
NOTIFICATION_CMD = "aplay" # linux alsa 

TEST_MODE = False

## -------------------------------------------------
## CustomAxis
## -------------------------------------------------
class CustomAxis(pg.AxisItem):
    def __init__(self, *args, **kwargs):
        pg.AxisItem.__init__(self, *args, **kwargs)

    def tickStrings(self, values, scale, spacing):
        # result = super(pg.AxisItem, self).tickStrings(values, scale, spacing)
        #pprint(values)
        strings = [ ]
        for x in values:
            x -= START_INTERVAL
            if x < 0:
                strings.append("")
            else:
                strings.append(str(int(x / 10.0)))
                #print x
                #if x % BENCHMARK_INTERVAL == 0:
                    #strings.append(str(int(x / BENCHMARK_INTERVAL)))
                #else:
                    #strings.append("")
        ## FOR
        return strings
        #return (values)
        
        
        #strings = []
        #for v in values:
            ## vs is the original tick value
            #vs = v * scale
            ## if we have vs in our values, show the string
            ## otherwise show nothing
            #if vs in self.x_values:
                ## Find the string with x_values closest to vs
                #vstr = self.x_strings[np.abs(self.x_values-vs).argmin()]
            #else:
                #vstr = ""
            #strings.append(vstr)
        #return strings
## CLASS
    
## -------------------------------------------------
## UPDATE PLOT
## -------------------------------------------------
def updatePlot():
    global p, PLOT_DATA, PLOT_CURVE, PLOT_LOCATION, PLOT_THROUGHPUT, PLOT_THROUGHPUT_AMOUNT, FINISHED
    lastPoint = PLOT_DATA[-1]

    # Get the new data point
    nextPoint = None
   
    if not FINISHED:
        try:
            nextPoint = NEW_DATA.pop(0)
        except:
            nextPoint = lastPoint
    else:
        nextPoint = 0
    
    PLOT_DATA[:-1] = PLOT_DATA[1:]  # shift PLOT_DATA in the array one sample left
    if TEST_MODE:
        PLOT_DATA[-1] = max(100, lastPoint + random.randint(-10, 10))
        if PLOT_LOCATION % 100 == 0: print "PLOT_LOCATION:", PLOT_LOCATION
    else:
        PLOT_DATA[-1] = nextPoint
    PLOT_LOCATION += 1
    
    # Always make the first point zero to prevent the y-axis
    # from auto-scaling
    #PLOT_DATA[0] = 0
    PLOT_CURVE.setData(PLOT_DATA)
    PLOT_CURVE.setPos(PLOT_LOCATION, 0)

    # Vertical Lines
    to_remove = [ ]
    for line_pos in PLOT_LINE_POSITIONS:
        if line_pos >= PLOT_LOCATION and line_pos <= (PLOT_LOCATION + START_INTERVAL):
            linePen = pg.mkPen('y', width=PLOT_LINE_WIDTH)
            l = p.addLine(x=line_pos, pen=linePen)
            to_remove.append(line_pos)
            PLOT_LINES[line_pos] = l
    ## FOR
    map(PLOT_LINE_POSITIONS.remove, to_remove)
    to_remove = [ ]
    for line_pos in PLOT_LINES.keys():
        if line_pos < PLOT_LOCATION:
            to_remove.append(line_pos)
            p.removeItem(PLOT_LINES[line_pos])
    ## FOR
    map(PLOT_LINES.pop, to_remove)

    if PLOT_THROUGHPUT_AMOUNT != None and not FINISHED:
        p.setTitle("<font size=\"64\">%d txn/sec</font>" % PLOT_THROUGHPUT_AMOUNT)
    elif FINISHED:
        p.setTitle("<font size=\"64\">-- txn/sec</font>")
        PLOT_THROUGHPUT_AMOUNT = None
    
        #if nextPoint == PLOT_THROUGHPUT_AMOUNT and not nextPoint is None:
            #PLOT_THROUGHPUT.setText("Throughput: %.1f txn/sec" % PLOT_THROUGHPUT_AMOUNT)
        #elif PLOT_THROUGHPUT_AMOUNT is not None and FINISHED:
            #PLOT_THROUGHPUT.setText("Throughput: --")
    ### IF
        
## UPDATE PLOT

## -------------------------------------------------
## EXECUTE BENCHMARK
## -------------------------------------------------
def execBenchmark():
    global p, NEW_DATA, FINISHED, PLOT_THROUGHPUT_AMOUNT
    results = oltpbench.executeBenchmark(create=False, \
                                load=False, \
                                execute=True, \
                                interval=BENCHMARK_INTERVAL)
    steps = BENCHMARK_INTERVAL / float(GRAPH_INTERVAL)
    for nextPoint in results:
        # Smooth interpolation
        lastPoint = PLOT_DATA[-1]
        step = (nextPoint - lastPoint) / steps
        for i in xrange(int(steps)):
            lastPoint += step
            NEW_DATA.append(lastPoint)
        ## FOR
        PLOT_THROUGHPUT_AMOUNT = nextPoint
    ## FOR (iterator)
    FINISHED = True
    #notification()
## DEF

## -------------------------------------------------
## NOTIFICATION
## -------------------------------------------------
def notification():
    cmd = [ NOTIFICATION_CMD, NOTIFICATION_SOUND ]
    subprocess.call(cmd)
## DEF

## -------------------------------------------------
## POLL PELOTON LOG
## -------------------------------------------------
def pollPelotonLog():
    global PLOT_LOCATION, NUM_INDEXES, EXPECTED_INDEXES
    regex = re.compile(".*?INFO[\s]+-[\s]+Enabling index[\s]+:[\s]+([\w]+)")
    results = oltpbench.pollFile("/tmp/peloton.log")
    for line in results:
        # Look for when we add an index
        m = regex.search(line)
        #print "XXX", line
        if m:
            #print "FOUND:", m.groups()[0]
            PLOT_LINE_POSITIONS.add(START_INTERVAL + PLOT_LOCATION)
            NUM_INDEXES += 1
            addIndex(m.groups()[0])
            if NUM_INDEXES == EXPECTED_INDEXES: notification()
        if FINISHED: break
    ## FOR (iterator)
## DEF

def addIndex(name):
    global INDEX_MAP
    name = name.strip().upper()
    info = INDEX_MAP[name]
    params = {
        "table": info[0],
        "index": name,
        "type": "Bw-Tree",
        "cols": ",".join(info[1:])
    }
    remoteCmd = "./add-index.sh %(table)s %(index)s %(type)s '(%(cols)s)'" % params
    cmd = [ "ssh", "dev4", remoteCmd ]
    subprocess.call(cmd)
    return
## DEF


## Start Qt event loop unless running in interactive mode or using pyside.
if __name__ == '__main__':
    
    # Initialize graph
    win = pg.GraphicsWindow(title="Peloton")
    win.resize(PLOT_WIDTH, PLOT_HEIGHT)

    customAxis = CustomAxis(orientation='bottom')
    #plot = win.addPlot()
    p = win.addPlot(axisItems={'bottom': customAxis})
    titleStyle = {'color': '#FFF', 'font-size': '50pt'}
    p.setTitle("<font size=\"64\">Self-Driving Demo</font>", **titleStyle)

    labelStyle = {} # {'color': '#FFF', 'font-size': '14pt'}
    p.setLabel('left', "Throughput", units='txn/sec', **labelStyle) 
    p.setLabel('bottom', "Elapsed Time", **labelStyle) 
    #ax = p.getAxis('bottom')

    limits = {'yMin': 0}
    p.setLimits(**limits)
    #if not TEST_MODE:
        #p.setRange(yRange=[0,40000])
    #print p
    #pprint(dir(p))
    #sys.exit(1)
    
    if TEST_MODE:
        for x in xrange(10):
            PLOT_LINE_POSITIONS.add(START_INTERVAL + 100 + (10 * x))
    
    #PLOT_THROUGHPUT = pg.TextItem(text="Throughput: --", border='w', anchor=(1,1))
    #PLOT_THROUGHPUT.setPos(0, 0)
    #p.addItem(PLOT_THROUGHPUT)
    
    PLOT_CURVE = p.plot(PLOT_DATA, fillLevel=-0.3, brush=(50,50,200,100))
    #PLOT_CURVE.setPen(color=(40,54,83), width=3)
    PLOT_CURVE.setPen(width=3)
    
    timer = pg.QtCore.QTimer()
    timer.timeout.connect(updatePlot)
    timer.start(100)
    
    threads = [ ]
    
    # Start Peloton log monitor
    threads.append(threading.Thread(target=pollPelotonLog))
    threads[-1].setDaemon(True)
    if not TEST_MODE: threads[-1].start()
    
    # Start OLTP-Bench thread
    threads.append(threading.Thread(target=execBenchmark))
    threads[-1].setDaemon(True)
    if not TEST_MODE: threads[-1].start()
    
    def close():
        global threads
        pass
            
    
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        #sh = QtGui.QShortcut(QtGui.QKeySequence("Ctrl+C"),imv,None, close)
        #sh.setContext(QtCore.Qt.ApplicationShortcut)
        QtGui.QApplication.instance().exec_()
        
## MAIN
