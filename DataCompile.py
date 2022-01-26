# -*- coding: utf-8 -*-
"""
Created on Thu Dec 16 07:40:56 2021

@author: Bing.Li
"""
import os
import pandas as pd
import numpy as np
import logging

class DataCompile:

    # =========================================================================
    # the purpose of this class is to provide static helper functions for data compiliatoin
    # for example, extract top row from each portfolio report
    # ========================================================================= 
    
    @staticmethod
    def extractDataFromXLS(folder, col_names, summary_file):
        # extract the first line of each report and aggregate them together into one file
        
        # examples of input arguments:
        
        #folder = r"C:/Users/bing.li/OneDrive - AIMCo/Documents/PythonPrograms/Carlos' Draft Code/Output/"
        #summary_file = r"C:/Users/bing.li/OneDrive - AIMCo/Documents/PythonPrograms/Carlos' Draft Code/Output/summary.txt"
        
        #col_names = ["Composite Name",
        #     "Unnamed: 2",
        #     "Benchmark Name",
        #     "Relative Value At Risk 95% Standalone %",
        #     "Relative Value At Risk 99% Standalone %",
        #     "Relative Expected Tail Loss 95% Standalone %",
        #     "Relative Expected Tail Loss 99% Standalone %"]
        
         
         summary_file_o = open(summary_file, "a+")
         summary_file_o.write("\t".join(col_names) + "\n")

         for file in os.listdir(folder):
             if file.endswith(".xlsx"):
                 file_name = os.path.join(folder, file)
                 logging.info(file_name)
                 xl_file_dfs = pd.read_excel(file_name, sheet_name="Sheet 0")
                 xl_file_dfs = xl_file_dfs.replace(np.nan, '', regex=True)
                 data = xl_file_dfs[col_names].values[1]        
                 summary_file_o.write("\t".join(data) + "\n")
        
         summary_file_o.close()