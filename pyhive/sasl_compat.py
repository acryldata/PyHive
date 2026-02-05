# Original source of this file is https://github.com/cloudera/impyla/blob/master/impala/sasl_compat.py 
# which uses Apache-2.0 license as of 21 May 2023.
# This code was added to Impyla in 2016 as a compatibility layer to allow use of either python-sasl or pure-sasl 
# via PR https://github.com/cloudera/impyla/pull/179
# Even though thrift_sasl lists pure-sasl as dependency here https://github.com/cloudera/thrift_sasl/blob/master/setup.py#L34 
# but it still calls functions native to python-sasl in this file https://github.com/cloudera/thrift_sasl/blob/master/thrift_sasl/__init__.py#L82
# Hence this code is required for the fallback to work.
 

import struct

from puresasl.client import SASLClient, SASLError
from contextlib import contextmanager

@contextmanager
def error_catcher(self, Exc = Exception):
    try:
        self.error = None
        yield
    except Exc as e:
        self.error = str(e)


class PureSASLClient(SASLClient):
    def __init__(self, *args, **kwargs):
        self.error = None
        super(PureSASLClient, self).__init__(*args, **kwargs)

    def start(self, mechanism):
        with error_catcher(self, SASLError):
            if isinstance(mechanism, list):
                self.choose_mechanism(mechanism)
            else:
                self.choose_mechanism([mechanism])
            return True, self.mechanism, self.process()
        # else
        return False, mechanism, None

    def encode(self, outgoing):
        """
        Encode (wrap) outgoing data for secure transmission to the server.
        
        This method is called by thrift_sasl when sending data with QOP
        integrity (auth-int) or confidentiality (auth-conf) protection.
        
        thrift_sasl expects encode() to return data with a 4-byte big-endian
        length header prepended (per RFC 4422 SASL framing). python-sasl's
        encode() includes this header via Cyrus SASL, but pure-sasl's wrap()
        returns only the wrapped payload, so we must add the header here.
        
        Note: Prior to this fix, encode() incorrectly called unwrap() instead
        of wrap(), and decode() called wrap() instead of unwrap(). This bug
        existed since Feb 2016 (cloudera/impyla PR #179) but only manifests
        when using pure-sasl with GSSAPI and auth-int/auth-conf QOP modes.
        """
        with error_catcher(self):
            wrapped = self.wrap(outgoing)
            # Add 4-byte big-endian length header as thrift_sasl expects
            return True, struct.pack(">I", len(wrapped)) + wrapped
        # else
        return False, None

    def decode(self, incoming):
        """
        Decode (unwrap) incoming data received from the server.
        
        This method is called by thrift_sasl when receiving data with QOP
        integrity (auth-int) or confidentiality (auth-conf) protection.
        
        thrift_sasl passes the 4-byte length header along with the payload
        to decode(). We must strip this header before calling unwrap().
        """
        with error_catcher(self):
            # thrift_sasl passes header + data, skip the 4-byte length header
            return True, self.unwrap(incoming[4:])
        # else
        return False, None

    def step(self, challenge=None):
        with error_catcher(self):
            return True, self.process(challenge)
        # else
        return False, None

    def getError(self):
        return self.error
