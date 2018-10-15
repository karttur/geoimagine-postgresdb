'''
Created on 10 Oct 2018

@author: thomasgumbricht
'''

from geoimagine.postgresdb import PGsession
from base64 import b64encode
import netrc

#from geoimagine.support.karttur_dt import Today

class ManageSMAP(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'managesmap'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageSMAP')
        
