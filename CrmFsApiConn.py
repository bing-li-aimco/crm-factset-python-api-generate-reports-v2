# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 19:16:22 2021

@author: Bing.Li
"""

#from fds.analyticsapi.engines.api import configurations_api
from fds.analyticsapi.engines.configuration import Configuration
from fds.analyticsapi.engines.api_client import ApiClient
from fds.analyticsapi.engines.api.components_api import ComponentsApi

class CrmFsApiConn:
    # =========================================================================
    # define a class that contains basic FS API settings 
    # and connect to the api
    # =========================================================================    
    def __init__(self, hst, usn, pwd):
        # input the most basic information
        # set up configuration
        self.host = hst
        self.username = usn
        self.password = pwd
        self.config = None
        self.api_client = None
        self.components_api = None
    
    
    def connectFsApi(self):
        # connect to the host
        # set up api
        self.config = Configuration()
        self.config.host = self.host
        self.config.username = self.username
        self.config.password = self.password
        self.api_client = ApiClient(self.config)
        self.components_api = ComponentsApi(self.api_client)
    
        
    