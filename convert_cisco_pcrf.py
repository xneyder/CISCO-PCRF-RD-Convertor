#!/usr/bin/python
# convert_cisco_pcrf.py:
#
# Description: 	This process runs on the background constanly checking the
#       input dirs for new raw data files, then it converts the file to the
#		output format
#
# Input Format Example:
#		D,Site2-oam01,cpu.0.idle,1636350946
#		D,Site2-oam01,cpu.0.interrupt,78513
#		D,Site2-oam01,cpu.0.nice,2372317
#		D,Site2-oam01,cpu.0.softirq,637517
#
# Ouput Format Example:
#		ne_name,om_group,cpu.id,cpu.idle,cpu.interrupt,cpu.nice,cpu.softirq
#		Site2-oam01,cpu,1636350946,78513,2372317,637517
#
# Input Parameters:
#		INPUT_FOLDER_LIST: Space delimited list with the input raw data folders 
#
# Example:
#		convert_cisco_pcrf.py "/raw_data/inputs/ /raw_data/backlog/"
#
# Output:
#		Converted raw data files created in the same folder as the input raw
#       data file.
#
# Database:	N/A
#
# Created by : Daniel Jaramillo
# Creation Date: 29/10/2018
# Modified by:     Date:
# All rights(C) reserved to Teoco
###########################################################################
import sys
import os
import time
import glob
import pandas as pd
from threading import Thread
from LoggerInit import LoggerInit

#Load the configuration from HLD file
def load_hld(hld_file):
    app_logger=logger.get_logger("load_hld "+hld_file)
    app_logger.info("Loading configuration")
    xl=pd.ExcelFile(hld_file)
    df=xl.parse('Counters')
    print(df.iloc[2:,[2,3,4]])



#Description: running in a thread to process the files found in the folder
#Input Parametes:
#    folder: path to the raw data files
def process_folder(folder):
    app_logger=logger.get_logger("process_folder "+folder)
    cycle_interval=60
    file_mask=os.path.join(filesfolder,"pre*")
    while True:
        app_logger.info("Looking for new files")
        file_list=glob.glob(file_mask)
        #Sleep before rocessing the files to make sure they are closed
        app_logger.info("Sleeping {cycle_interval}"
                        .format(cycle_interval=cycle_interval))
        time.sleep(cycle_interval)
        for file_name in file_list:
            app_logger.info("Processing file {file_name}"
                            .format(file_name=file_name))
            with open(file_name) as file:
                lines=file.read().split('\n')
                for line in lines:
                    arr_line=line.split(',')
                    ne_name=arr_line[1]
                    rd_counter_name=arr_line[2]
                    counter_value=arr_line[3]
                    #Check if the counter name is found in the list

def main():
    app_logger=logger.get_logger("main")
    app_logger.info("Starting {script}".format(script=sys.argv[0]))
    #Validate the line arguments
    if len(sys.argv) < 2:
        app_logger.error("Usage {script} 'input folder list'"
                         .format(script=sys.argv[0]))
        app_logger.error("Example {script} '/raw_data/inputs/ /raw_data/bac/'"
                         .format(script=sys.argv[0]))
        quit()
    input_folders=sys.argv[1].split(' ')
    load_hld(hld_file)
    quit()
    workers=[]
    #Start a thread for each input folder and keep track of it in workers
    for folder in input_folders:
        worker = Thread(target=process_folder, args=(folder,))
        worker.setDaemon(True)
        workers.append({'function':process_folder,'params':folder
                        ,'object':worker})
        worker.start()

    #Monitor that all the threads are running
    while True:
        for idx,running_worker in enumerate(workers):
            #Thread is not alive restart it
            if not running_worker['object'].isAlive():
                app_logger.error('Thread {running_worker} crashed running it' +
                                 'again'.format(running_worker=running_worker))
                worker = Thread(target=running_worker['function']
                                    , args=(running_worker['params'],))
                worker.setDaemon(True)
                workers[idx]['object']=worker
                worker.start()
        time.sleep(900)

#Script starts running here
if __name__ == "__main__":
    #If LOG_DIR environment var is not defined use /tmp as logdir
    if 'LOG_DIR' in os.environ:
        log_dir=os.environ['LOG_DIR']
    else:
        log_dir="/tmp"

    log_file=os.path.join(log_dir,"convert_cisco_pcrf.log")
    logger=LoggerInit(log_file,10)
    hld_file="HLD-CISCO_PCRF_FPP.xls"
    main()
