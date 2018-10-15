'''
Created on 21 feb. 2018

@author: thomasgumbricht
'''

from geoimagine.postgresdb import PGsession
from base64 import b64encode
import netrc

class SelectUser(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """The constructor connects to the database"""
        #Connect to the Postgres Server with 'readuser        
        HOST = 'localhost89'
        secrets = netrc.netrc()
        dbuser, account, dbuserpswd = secrets.authenticators( HOST )
        dbuserpswd = b64encode(dbuserpswd.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':dbuser,'pswd':dbuserpswd}
        PGsession.__init__(self,query,'SelectUser')
        
    def _SelectUserCreds(self, userid, pswd):
        '''
        self.useridD = useridD
        if not useridD['userid']:
            return False
        '''
        if len(pswd) > 8:

            self.userpswd = pswd
            self.userid = userid
        else:   
            HOST = userid
            secrets = netrc.netrc()
            secrets = secrets.authenticators( HOST )
            if secrets == None:
                exitstr = 'No authentication found for %(a)s' %{'a':HOST} 
                exit(exitstr)
            self.userid, self.account, self.userpswd = secrets
            
        query = {'id': self.userid, 'pswd':self.userpswd}
        self.cursor.execute("SELECT userid,usercat,stratum FROM userlocale.users WHERE userid = '%(id)s' and userpswd = '%(pswd)s';" %query)
        rec = self.cursor.fetchone()
        if rec == None:
            return rec
        self.cursor.execute("SELECT projid FROM userlocale.userprojects WHERE owner = '%(id)s';" %query)
        userProjs = self.cursor.fetchall()
        self.cursor.execute("SELECT tractid FROM regions.tracts WHERE owner = '%(id)s';" %query)
        userTracts = self.cursor.fetchall()
        self.cursor.execute("SELECT siteid FROM regions.sites WHERE owner = '%(id)s';" %query)
        userSites = self.cursor.fetchall()
        return (rec[0], rec[1], rec[2], userProjs, userTracts, userSites)
    