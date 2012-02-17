# -*- coding: utf-8 -*-

import socket

'''
Convert the user host field of the MySQL query log trace to extract the IP
address for addition to the session object
'''
def stripIPtoUnicode(sql_string) :
    l = sql_string.rfind('[') + 1;
    r = sql_string.rfind(']');
    ip = sql_string[l:r]
    if (ip == '') :
        return u'127.0.0.1'
    else :
        return unicode(ip)
    ## ENDIF
## ENDDEF

'''
Detect the host IP address
'''
def detectHostIP() :
    return unicode(socket.gethostbyname(socket.gethostname()))
## ENDDEF
