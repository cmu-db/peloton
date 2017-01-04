#!/usr/bin/env python

import sys
import os
import random
import threading
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

PLOT_DATA = [0]*START_INTERVAL
PLOT_LOCATION = 0
PLOT_CURVE = None
PLOT_THROUGHPUT = None
PLOT_THROUGHPUT_AMOUNT = None
NEW_DATA = [ ]

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
    global PLOT_DATA, PLOT_CURVE, PLOT_LOCATION, PLOT_THROUGHPUT, PLOT_THROUGHPUT_AMOUNT, FINISHED
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
    # PLOT_DATA[-1] = max(100, last + random.randint(-10, 10)) # np.random.normal()
    PLOT_DATA[-1] = nextPoint
    PLOT_LOCATION += 1
    PLOT_CURVE.setData(PLOT_DATA)
    PLOT_CURVE.setPos(PLOT_LOCATION, 0)
    
    if not PLOT_THROUGHPUT is None:
        if nextPoint == PLOT_THROUGHPUT_AMOUNT and not nextPoint is None:
            PLOT_THROUGHPUT.setText("Throughput: %.1f txn/sec" % PLOT_THROUGHPUT_AMOUNT)
        elif PLOT_THROUGHPUT_AMOUNT is not None and FINISHED:
            PLOT_THROUGHPUT.setText("Throughput: --")
    ## IF
        
## UPDATE PLOT

## -------------------------------------------------
## EXECUTE BENCHMARK
## -------------------------------------------------
def execBenchmark():
    global NEW_DATA, FINISHED, PLOT_THROUGHPUT_AMOUNT
    results = oltpbench.execute(create=False, load=False, execute=True)
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
## DEF


## Start Qt event loop unless running in interactive mode or using pyside.
if __name__ == '__main__':
    # Initialize graph
    win = pg.GraphicsWindow(title="Peloton")
    #win.setWindowFlags(win.windowFlags() | QtCore.Qt.FramelessWindowHint)
    win.resize(1000,300)

    customAxis = CustomAxis(orientation='bottom')
    #plot = win.addPlot()
    p = win.addPlot(axisItems={'bottom': customAxis})
    titleStyle = {'color': '#FFF', 'font-size': '50pt'}
    p.setTitle("<font size=\"64\">Self-Driving Demo</font>", **titleStyle)

    labelStyle = {'color': '#FFF', 'font-size': '16pt'}
    p.setLabel('left', "Throughput", units='txn/sec', **labelStyle) 
    p.setLabel('bottom', "Elapsed Time", **labelStyle) 
    #ax = p.getAxis('bottom')

    limits = {'yMin': 0}
    p.setLimits(**limits)
    p.setRange(yRange=[0,200])
    #print p
    #pprint(dir(p))
    #sys.exit(1)
    
    PLOT_THROUGHPUT = None # pg.TextItem(text="Throughput: --", border='w')
    #p.addItem(PLOT_THROUGHPUT)
    #PLOT_THROUGHPUT.setPos(0, 0)
    
    PLOT_CURVE = p.plot(PLOT_DATA)
    #PLOT_CURVE.setPen(color=(40,54,83), width=3)
    PLOT_CURVE.setPen(width=3)
    
    timer = pg.QtCore.QTimer()
    timer.timeout.connect(updatePlot)
    timer.start(100)
    
    
    # Start OLTP-Bench thread
    t = threading.Thread(target=execBenchmark)
    t.start()
    
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtGui.QApplication.instance().exec_()
        
## MAIN