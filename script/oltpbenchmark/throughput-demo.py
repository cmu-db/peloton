#!/usr/bin/env python

import sys
import os
import random
import subprocess
import numpy as np
import pyqtgraph as pg
from pyqtgraph.Qt import QtGui, QtCore
from pprint import pprint

# Our API
from oltpbench import execute

# Enable antialiasing for prettier plots
pg.setConfigOptions(antialias=True)
#pg.setConfigOptions(useOpenGL=True)

#QtGui.QApplication.setGraphicsSystem('raster')
app = QtGui.QApplication([])
#mw = QtGui.QMainWindow(parent=None, flags=QtCore.Qt.FramelessWindowHint)
#mw.resize(800,800)


PLOT_DATA = [0]*300
PLOT_LOCATION = 0

#def initGraph():
    #global PLOT_DATA, PLOT_LOCATION
    

win = pg.GraphicsWindow(title="Peloton Demo")
#win.setWindowFlags(win.windowFlags() | QtCore.Qt.FramelessWindowHint)
win.resize(1000,300)

p = win.addPlot()
titleStyle = {'color': '#FFF', 'font-size': '64pt'}
p.setTitle("<font size=\"64\">Peloton Demo</font>", **titleStyle)

labelStyle = {'color': '#FFF', 'font-size': '16pt'}
p.setLabel('left', "Throughput", units='txn/sec', **labelStyle) 
#print p.getLabel('left')
#print p
#pprint(dir(p))
#sys.exit(1)

curve = p.plot(PLOT_DATA)
#curve.setPen(color=(40,54,83), width=3)
curve.setPen(width=3)
PLOT_LOCATION = 0
def updatePlot():
    global PLOT_DATA, curve, PLOT_LOCATION
    last = PLOT_DATA[-1]
    PLOT_DATA[:-1] = PLOT_DATA[1:]  # shift PLOT_DATA in the array one sample left
    PLOT_DATA[-1] = max(100, last + random.randint(-10, 10)) # np.random.normal()
    PLOT_LOCATION += 1
    curve.setData(PLOT_DATA)
    curve.setPos(PLOT_LOCATION, 0)
## UPDATE PLOT

timer = pg.QtCore.QTimer()
timer.timeout.connect(updatePlot)
timer.start(100)
## DEF


## Start Qt event loop unless running in interactive mode or using pyside.
if __name__ == '__main__':
    # Initialize graph
    #initGraph()
    
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtGui.QApplication.instance().exec_()
        
    print "???"