# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 20:06:44 2021

@author: Bing.Li
"""
import os
import time
import pandas as pd
import logging

from fds.analyticsapi.engines.api.components_api import ComponentsApi
from fds.analyticsapi.engines.model.pa_identifier import PAIdentifier
from fds.analyticsapi.engines.model.pa_date_parameters import PADateParameters
from fds.analyticsapi.engines.model.pa_calculation_parameters import PACalculationParameters
from fds.analyticsapi.engines.model.pa_calculation_parameters_root import PACalculationParametersRoot
from fds.analyticsapi.engines.api.pa_calculations_api import PACalculationsApi
from fds.analyticsapi.engines.exceptions import ApiException
from fds.analyticsapi.engines.model.component_summary import ComponentSummary
from fds.protobuf.stach.extensions.StachVersion import StachVersion
from fds.protobuf.stach.extensions.StachExtensionFactory import StachExtensionFactory

from CrmFsApiConn import CrmFsApiConn


class CrmFsPaReport(CrmFsApiConn):
    # =========================================================================
    # define a class that contains portfolio information
    # and includes main features of API reporting and handles
    # ========================================================================= 
    def __init__(self, hst, usn, pwd, val_date, pa_doc, port_bm_acct):
        super(CrmFsPaReport, self).__init__(hst, usn, pwd)
        super(CrmFsPaReport, self).connectFsApi()
        self.val_date = str(val_date)
        self.pa_document = pa_doc
        self.portfolio_benchmark_acct = port_bm_acct
        self.components = None
        self.component_ids = []
        self.table_names = {}
        
    def getComponentsFromPAdoc(self):
        # get the components from a PA document
        # return the table list
        self.components = self.components_api.get_pa_components(document=self.pa_document)
        self.component_ids = [id for id in self.components[0].data.keys()]
        self.table_names = {id:self.components[0].data[id].category+' - '+self.components[0].data[id].name for id in self.components[0].data.keys()}
    
    #def filterComponents(self):
        # TBD
        
    def generateReports(self, component_detail='TOTALS', using_cache='NO'):
        for port_name, port_acc, bm_acc, report_file in zip(self.portfolio_benchmark_acct['portfolio_name'], 
                                                            self.portfolio_benchmark_acct['portfolio_account'], 
                                                            self.portfolio_benchmark_acct['benchmark_account'], 
                                                            self.portfolio_benchmark_acct['report_filename']):
            try:
                self.generateReportForOnePortfolio(port_name, port_acc, bm_acc, report_file, component_detail, using_cache)
            except Exception as e:
                logging.error(str(e))
                continue
        

    
    def generateReportForOnePortfolio(self, portfolio_name, portfolio_acc, benchmark_acc, output_filename, component_detail, using_cache):

        # if directory not exist --> create one
        # relative path for output files
        dir_path = os.path.dirname(os.path.realpath(__file__))    
        output_filename = dir_path + output_filename
        outputFileDir = os.path.dirname(output_filename)
        if not os.path.exists(outputFileDir):
            os.makedirs(outputFileDir)
        
        cache_control = 'max-stale=0' if using_cache.upper() == 'NO' else 'max-stale=43200'
               
        self.FactSet_PA_API(portfolio_acc, benchmark_acc, None, self.val_date, "Single", component_detail, cache_control, output_filename)
        logging.info('Complete writing out reports to '+output_filename)  
        logging.info('=============================================================')
    
    
    def FactSet_PA_API(self, pa_portfolio, pa_benchmark, startdate, enddate, frequency, pa_comp_detail, cache_control, output_filename):
        # =========================================================================
        # This function is originally from FactSet API help doc
        # Further enhanced by Bing Li 
        # including log, class, and while statements affecting time, and multi calc, table name, hierarchy level. 
        # =========================================================================
        
        pa_accounts = [PAIdentifier(id=pa_portfolio)]
        pa_benchmarks = [PAIdentifier(id=pa_benchmark)]
        if startdate == None:
            pa_dates = PADateParameters(enddate=enddate, frequency=frequency)
        else:
            pa_dates = PADateParameters(enddate=enddate, frequency=frequency, startdate=startdate)

        pa_calculation_parameters = {}
        for count, value in enumerate(self.component_ids):
            pa_calculation_parameters[str(count)] = PACalculationParameters(componentid=value, 
                                                                             accounts=pa_accounts, 
                                                                             benchmarks=pa_benchmarks, 
                                                                             dates=pa_dates,
                                                                             componentdetail=pa_comp_detail)
        
        logging.info('=============================================================')
        #logging.info(pa_calculation_parameters)

        pa_calculation_parameter_root = PACalculationParametersRoot(data=pa_calculation_parameters)
        logging.info('PA parameters setup completed.')
        pa_calculations_api = PACalculationsApi(self.api_client)
        post_and_calculate_response = pa_calculations_api.post_and_calculate(pa_calculation_parameters_root=pa_calculation_parameter_root,
                                                                             cache_control=cache_control)
        logging.info('Post and calculate...')

        if post_and_calculate_response[1] == 201:
            # expected response if the calculation has one unit and is completed in a pre-defined span
            tables = self.output_calculation_result(post_and_calculate_response[0]['data'])
            table_df = tables[0]
            table_df.insert(loc=0, column='Table_Name', value=self.table_names[self.component_ids[0]])
            writer = pd.ExcelWriter(output_filename, engine='xlsxwriter', options={'strings_to_numbers': True})
            dataFrame.to_excel(writer, 'Sheet 0', index=False)
            writer.close()
            
        #elif post_and_calculate_response[1] == 200:
        #    # expected response if the calculation has one unit and is completed with an error
        #    for (calculation_unit_id, calculation_unit) in post_and_calculate_response[0].data.units.items():
        #        logging.error("Calculation Unit Id: " + calculation_unit_id + " Failed!!!")
        #        logging.info("Error message : " + str(calculation_unit.errors))
        elif post_and_calculate_response[1] == 202 or post_and_calculate_response[1] == 200:
            # expected response for multi calculation
            calculation_id = post_and_calculate_response[0].data.calculationid
            logging.info("Calculation Id: " + calculation_id)

            status_response = pa_calculations_api.get_calculation_status_by_id(id=calculation_id)

            acc_age = 0
            while status_response[1] == 202 and (status_response[0].data.status in ("Queued", "Executing")):
                max_age = '5'
                #cache_control_get = status_response[2].get("cache-control")
                connection_get = status_response[2].get("Connection")
                acc_age += int(max_age)
                time.sleep(int(max_age))
                
                if(acc_age % 60 ==0 or acc_age <= 10):
                    # only show time elapsed over certain time interval - not too frequent
                    logging.info(status_response[0].data.status+' - time elapsed: '+str(acc_age)+'s, '+
                                 'Connection: '+connection_get)
                
                status_response = pa_calculations_api.get_calculation_status_by_id(id=calculation_id)
            
            
            logging.info('Post and calculate completed --> preparing write out...')
            writer = pd.ExcelWriter(output_filename, engine='xlsxwriter', options={'strings_to_numbers': True})
            for (calculation_unit_id, calculation_unit) in status_response[0].data.units.items():
                if calculation_unit.status == "Success":
                    table_name = self.table_names[self.component_ids[int(calculation_unit_id)]]
                    logging.info("Calculation Unit Id: "+calculation_unit_id+' - '+table_name+" Succeeded!!!")
                    result_response = pa_calculations_api.get_calculation_unit_result_by_id(id=calculation_id,
                                                                                            unit_id=calculation_unit_id)
                    tables = self.output_calculation_result(result_response[0]['data'])
                    table_df = tables[0]
                    table_df.insert(loc=0, column='Table_Name', value=table_name)
                    table_df.to_excel(writer, 'Sheet '+calculation_unit_id, index=False)

                    
                else:
                    logging.error("Calculation Unit Id:" + calculation_unit_id +' - '+table_name+ " Failed!!!")
                    logging.info("Error message : " + str(calculation_unit.errors)) 
            
            writer.close()
        else:
            raise Exception('Error: post_calculate_repsonse '+post_and_calculate_response[1])
            
        #return tables
        
    def output_calculation_result(self, result):
        #logging.info("Calculation Result")
        stachBuilder = StachExtensionFactory.get_row_organized_builder(StachVersion.V2)
        stachExtension = stachBuilder.set_package(result).build()
        dataFramesList = stachExtension.convert_to_dataframe()
        return dataFramesList
        # generate_excel(dataFramesList)  # Uncomment this line to get the result in table format exported to excel file.
    '''
    def generate_excel(self, dataFrame, sheet_name, output_filename):
        writer = pd.ExcelWriter(output_filename, engine='xlsxwriter', options={'strings_to_numbers': True})
        dataFrame.to_excel(writer, sheet_name, index=False)
        writer.close()
    '''