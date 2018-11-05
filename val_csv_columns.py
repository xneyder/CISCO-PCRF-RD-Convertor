#!/usr/bin/python
# val_csv_columns.py:
#
# Description:  Utility process to validate is a csv file chages the number of
# columns between rows
#
# Input Format Example:
#       D,Site2-oam01,cpu.0.idle,1636350946
#       D,Site2-oam01,cpu.0.interrupt,78513
#       D,Site2-oam01,cpu.0.nice,2372317
#       D,Site2-oam01,cpu.0.softirq,637517
#
# Input Parameters:
#       INPUT_FOLDER: Location of the csv files
#
# Example:
#       val_csv_columns.py "/raw_data/inputs/CISCO*"
#
# Output:
#       Prints the number of columns found in the file and an ERROR is the
#       colum size changes
#
# Database: N/A
#
# Created by : Daniel Jaramillo
# Creation Date: 29/10/2018
# Modified by:     Date:
# All rights(C) reserved to Teoco
###########################################################################
import sys
import glob

def main():
    if len(sys.argv) < 2:
        app_logger.error("Usage {script} 'input files mask'"
                         .format(script=sys.argv[0]))
        app_logger.error("Example {script} '/tmp/input/CISCO*'"
                         .format(script=sys.argv[0]))
        quit()
    file_mask=sys.argv[1]

    for filename in glob.glob(file_mask):
        print(filename)
        error=False
        with open(filename,'r') as file:
            filedata=file.read().split('\n')
            prev_cols=None
            for idx in range(2,len(filedata)):
                if len(filedata[idx].split(',')) == 1:
                    continue
                cols=len(filedata[idx].split(','))
                if prev_cols and cols!=prev_cols:
                    print("""ERROR {line} Line {line_idx} # of columns changed
                          to {cols}"""
                          .format(line=filedata[idx],cols=cols,line_idx=idx+1))
                    error=True
                prev_cols=cols
            if not error:
                print("OK")

if __name__ == "__main__":
    main()
