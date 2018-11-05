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
import re
import time
import glob
import pandas as pd
from threading import Thread
from LoggerInit import LoggerInit

####
#Description: Load the configuration from HLD file
#Input Parametes:
#    hld_file: Excel containing the functional specification for the library
def load_hld(hld_file):
    app_logger=logger.get_logger("load_hld "+hld_file)
    app_logger.info("Loading configuration")
    global regexp_list
    global metadata
    global pi_key_list
    global ct_key_list
    global counter_list
    xl=pd.ExcelFile(hld_file)
    df=xl.parse('Counters')
    #select columns 2,3,4,6 and remove the nulls in Vendor Counter Name
    df=df.iloc[2:,[1,3,4,6]].dropna(subset=['Vendor Counter Name'])
    opt_counter_regexp=ur"(\[.+?\][_.]{0,1})"
    key_regexp=ur"(\<.+?\>)"
    for index,row in df.iterrows():
        om_group=row[0].strip()
        db_name=row[1].strip()
        rd_name=row[2].strip()
        _type=row[3].strip()
        #First time to see this om group add it to the dictionary and
        if om_group not in pi_key_list:
            pi_key_list[om_group]={}
        #if type is PI means that is a key field otherwise is regular counter
        if _type == "PI":
            for idx,key in enumerate(re.findall(key_regexp,rd_name)):
                #If there is onlye one key ignore the idx in the name
                if idx>0:
                    key_db_name=db_name+"_"+str(idx)
                else:
                    key_db_name=db_name
                pi_key_list[om_group][key]=key_db_name
        else:
            #Get the keys in the counter_name
            if om_group not in ct_key_list:
                ct_key_list[om_group]=[]
                for key in re.findall(key_regexp,rd_name):
                    #Get the db_name of the key foun in pi_key_list
                    ct_key_list[om_group].append(pi_key_list[om_group][key])

            #initialize it with empty arrays
            if om_group not in counter_list:
                counter_list[om_group]=set()
            re_name_list=[rd_name]
            #Check if counter has optional key if there are create a new
            #regexp removing the optional key
            optional_key_list=re.findall(opt_counter_regexp,rd_name)
            if optional_key_list:
                full_re_name_no_opt=rd_name
                for optional_key in optional_key_list:
                    re_name_no_opt=rd_name
                    re_name_no_opt=re_name_no_opt.replace(optional_key,"")
                    full_re_name_no_opt=full_re_name_no_opt.replace(
                        optional_key,""
                    )
                    re_name_list.append(re_name_no_opt)
                    re_name_list.append(full_re_name_no_opt)
            re_name_list=set(re_name_list)
            #switch the key value in the pi_key_list dict
            sw_pi_key_list={y:x for (x,y) in pi_key_list[om_group].items()}
            for re_name in re_name_list:
                #Scape all the dots
                re_name=re_name.replace('.','\\.')
                #removed the brackets used for optional counters
                re_name=re_name.replace('[','')
                re_name=re_name.replace(']','')
                #Replace all the keys in the counter name by (.*)
                local_key_list=[]
                for db_key in ct_key_list[om_group]:
                    key=sw_pi_key_list[db_key]
                    if key not in re_name:
                        local_key_list.append('NA')
                    else:
                        #Store the db name for the key
                        local_key_list.append(db_key)
                    re_name=re_name.replace(key,"(.*)")
                metadata[re_name]={
                        'key_list':local_key_list,
                        'db_name':db_name,
                        'om_group':om_group,
                    }
                counter_list[om_group].add(db_name)
    #create the list regexp_list with all the regular expresions
    regexp_list=[re_name for re_name in metadata.keys()]
    #sort the list by the length in reverse order
    regexp_list.sort(key = lambda s:len(s),reverse=True)
    #Compile the regexp list
    regexp_list=[re.compile(s) for s in regexp_list]

####
#Description: running in a thread to process the files found in the folder
#Input Parametes:
#    folder: path to the raw data files
def process_folder(folder):
    app_logger=logger.get_logger("process_folder "+folder)
    cycle_interval=60
    file_mask=os.path.join(folder,"pre*csv")
    while True:
        app_logger.info("Looking for new files")
        file_list=glob.glob(file_mask)
        #Sleep before rocessing the files to make sure they are closed
        app_logger.info("Sleeping {cycle_interval} seconds"
                        .format(cycle_interval=cycle_interval))
        time.sleep(cycle_interval)
        for file_name in file_list:
            out_data={}
            app_logger.info("Processing file {file_name}"
                            .format(file_name=file_name))
            with open(file_name,'r') as file:
                lines=file.read().split('\n')
                for line in filter(None,lines):
                    found_regexp=None
                    #split the line into the needed variables
                    try:
                        arr_line=line.split(',')
                        ne_name=arr_line[1].strip()
                        rd_counter_name=arr_line[2].strip()
                        counter_value=arr_line[3].strip()
                    except IndexError:
                        app_logger.error("{line} Incorrect format"
                                         .format(line=line))
                        continue
                    #Check if the counter name is found in the regexp_list
                    for regexp in regexp_list:
                        if regexp.match(rd_counter_name):
                            found_regexp=regexp
                            break
                    if not found_regexp:
                        #app_logger.error("{line} not found in the HLD"
                        #                 .format(line=line))
                        continue
                    #found_regexp holds an object we do need the pattern string
                    re_counter=found_regexp.pattern
                    #build the keys
                    counter_key_list=metadata[re_counter]['key_list']
                    key_value_list=found_regexp.findall(rd_counter_name)
                    #findall sometimes creates a list and sometimes creates
                    #a list of tuples we need everything as list
                    for key in key_value_list:
                        if type(key) is not str:
                            key_value_list=list(key)
                            break
                    #ne_name is the first key
                    out_key=[ne_name]
                    next_idx=0
                    for idx in range(len(counter_key_list)):
                        #If the key is optional and did not arrive put NA
                        if counter_key_list[idx]=='NA':
                            out_key.append('NA')
                        else:
                            #In case the key is not available put NA
                            key_val=key_value_list[next_idx]
                            if not key_val:
                                key_val="NA"
                            out_key.append(key_val)
                            next_idx+=1
                    out_key_str=','.join(out_key)
                    #check if the om_group for the counter found is already in
                    #out_data
                    om_group=metadata[re_counter]['om_group']
                    if om_group not in out_data:
                        out_data[om_group]={}
                    #Check if key exists for om_group
                    db_name=metadata[re_counter]['db_name']
                    if out_key_str not in out_data[om_group]:
                        out_data[om_group][out_key_str]={db_name:
                                                         counter_value}
                    else:
                        out_data[om_group][out_key_str][db_name]=counter_value
            #Build the output raw data files
            datetime=file_name.split('-')[3].split('.')[0]
            for om_group,data in out_data.items():
                out_file_name=om_group+"-"+os.path.basename(
                    file_name.replace("prev_",""))
                out_file_name=os.path.join(
                    os.path.dirname(file_name),out_file_name)
                with open(out_file_name,'w') as file:
                    #Add the name of the om_group
                    file.write("OM_GROUP: {om_group}\n".format(
                                   om_group=om_group
                    ))
                    #Add the datetime
                    file.write("DATETIME: {datetime}\n".format(
                                   datetime=datetime
                    ))
                    #add the ne_name to the header
                    file.write("NE_NAME")
                    #add to the header the name of the keys
                    for key in ct_key_list[om_group]:
                        file.write(",{key}".format(key=key))
                    #add to the header the name of the columns
                    for counter_name in counter_list[om_group]:
                        file.write(",{counter_name}".format(
                            counter_name=counter_name
                        ))
                    file.write("\n")
                    #add to the file the data
                    for key,counters in data.items():
                        file.write(key)
                        for counter_name in counter_list[om_group]:
                            if counter_name not in counters:
                                counter_value=''
                            else:
                                counter_value=counters[counter_name]
                            file.write(",{counter_value}".format(
                                counter_value=counter_value
                            ))
                        file.write("\n")
                        #Add end of file for parser to work in a full batch
                    file.write('#END#')
                app_logger.info("{out_file_name} file created"
                                .format(out_file_name=out_file_name))
            os.rename(file_name,file_name+"_")



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

    #Load configuration
    load_hld(hld_file)

    #Start a thread for each input folder and keep track of it in workers
    workers=[]
    for folder in input_folders:
        worker = Thread(target=process_folder, args=(folder,))
        worker.setDaemon(True)
        workers.append({'function':process_folder,'params':folder
                        ,'object':worker})
        worker.start()

    #Monitor that all the threads are running
    while True:
        for idx,running_worker in enumerate(workers):
            #If thread is not alive restart it
            if not running_worker['object'].isAlive():
                app_logger.error('Thread {running_worker} crashed running it' +
                                 'again'.format(running_worker=running_worker))
                worker = Thread(target=running_worker['function']
                                    , args=(running_worker['params'],))
                worker.setDaemon(True)
                workers[idx]['object']=worker
                worker.start()
        time.sleep(900)

#Application starts running here
if __name__ == "__main__":
    #If LOG_DIR environment var is not defined use /tmp as logdir
    if 'LOG_DIR' in os.environ:
        log_dir=os.environ['LOG_DIR']
    else:
        log_dir="/tmp"

    log_file=os.path.join(log_dir,"convert_cisco_pcrf.log")
    logger=LoggerInit(log_file,10)
    hld_file="HLD-CISCO_PCRF_FPP.xls"
    metadata={}
    pi_key_list={}
    ct_key_list={}
    counter_list={}
    regexp_list=[]
    main()
