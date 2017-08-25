#!/usr/env/ python

import time
import dask.dataframe as dd

t_ini=time.time()

LMC=dd.read_csv("result_b1nvtbs4rvurcyel.csv",sep=',', assume_missing=True,usecols=['t1.IAUNAME','t1.RAY','t1.DECY','t1.YPSFMAG','t1.YPSFMAGERR','t1.YSHARP','t1.RAJ','t1.DECJ','t1.JPSFMAG','t1.JPSFMAGERR','t1.JSHARP','t1.RAKS','t1.DECKS','t1.KSPSFMAG','t1.KSPSFMAGERR','t1.KSSHARP','t1.RA2000','t1.DEC2000'])


LMC.to_csv('table_LMC*.csv',sep=',' )


import os, glob

os.system("head -1 table_LMC001.csv > LMC_VISTA_YJKPSF.csv")
for file in sorted(glob.glob("table_LMC*.csv")):
    #print file
    os.system("tail -n +2 "+file+ "  >> LMC_VISTA_YJKPSF.csv")

print("--- %s seconds---"% (time.time()- t_ini))

