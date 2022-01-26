# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 20:06:44 2021

@author: Bing.Li
"""

import os
import getpass
import pandas as pd
import argparse
import logging
from datetime import date

from CrmFsPaReport import CrmFsPaReport


def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, default='crm_config.xlsx', help='The configuration file name that you put in the program folder.')
    args = parser.parse_args()
    
    # read from an excel config file for user and setting information
    # then creat an api connector and connect to api
    
    dir_path = os.path.dirname(os.path.realpath(__file__))  
    config_file = dir_path + '\\' + args.config
    xl_user = pd.read_excel(config_file, sheet_name="user")
    xl_setting = pd.read_excel(config_file, sheet_name="setting")
        
    # set up logging
    datestr = date.today().strftime('%Y%m%d')

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-6s %(levelname)-8s %(message)s',                        
                        datefmt='%m-%d %H:%M',
                        filename=dir_path+'/log_'+args.config.replace('.xlsx','_')+datestr+'.txt',
                        filemode='w')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(name)-6s %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)
    
    logging.info('=============================================================')
    logging.info('Program starts...')
    logging.info('Config file being used: '+config_file)
    logging.info('OS user: '+getpass.getuser())
    logging.info('FactSet API user name: '+xl_user['username'][0])
    
    for i in range(0, xl_setting.shape[0]):
        # loop through rows to generate reports for each row in the settings tab
        # better to not embed parallel computing at this level
        logging.info('=============================================================')
        logging.info('Start generating report for: ')
        logging.info('Valuation date: ' + str(xl_setting['val_date'][i]))
        logging.info('PA document: ' + xl_setting['pa_document'][i])
        logging.info('Portfolio name: ' + xl_setting['portfolio_name'][i])
        logging.info('Portfolio account: ' + xl_setting['portfolio_account'][i])
        logging.info('Benchmark account: ' + xl_setting['benchmark_account'][i])
        logging.info('Report filename: ' + xl_setting['report_filename'][i])
        logging.info('Hierarchy level: ' + xl_setting['hierarchy_level'][i])
        logging.info('Use cached result or not: ' + xl_setting['using_cache'][i])
                
        # create one instance for a report associated with one specific PA document
        this_portfolio = xl_setting.iloc[[i]][['portfolio_name', 'portfolio_account', 'benchmark_account', 'report_filename']]
        myReport = CrmFsPaReport(xl_user['host'][0], 
                                 xl_user['username'][0], 
                                 xl_user['password'][0], 
                                 xl_setting['val_date'][i], 
                                 xl_setting['pa_document'][i], 
                                 this_portfolio)
        
        # generate the report based on the input portfolio etc
        myReport.getComponentsFromPAdoc()
        myReport.generateReports(xl_setting['hierarchy_level'][i], xl_setting['using_cache'][i])
    
    logging.info('The program run is now completed - Please check the log and results.')
    logging.info('=============================================================')


if __name__ == "__main__":
    main()
    
