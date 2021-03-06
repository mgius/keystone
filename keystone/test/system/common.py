import unittest
import httplib

class HttpTestCase(unittest.TestCase):
    """Performs generic HTTP request testing"""

    def request(self, host='127.0.0.1', port=80, method='GET', path='/',
            headers={}, body=None, assert_status=None):
        """Perform request and fetch httplib.HTTPResponse from the server"""
        
        # Initialize a connection
        connection = httplib.HTTPConnection(host, port, timeout=3)

        # Perform the request
        connection.request(method, path, body, headers)
        
        # Retrieve the response so can go ahead and close the connection
        response = connection.getresponse()
        response.body = response.read()
        
        # Close the connection
        connection.close()
        
        # Automatically assert HTTP status code
        if assert_status:
            self.assertResponseStatus(response, assert_status)
        else:
            self.assertResponseSuccessful(response)
        
        # Contains the response headers, body, etc
        return response
    
    def assertResponseSuccessful(self, response):
        """Asserts that a status code lies inside the 2xx range"""
        self.assertTrue(response.status >= 200 and response.status <= 299,
            'Status code %d is outside of the expected range (2xx)\n\n%s' % 
            (response.status, response.body))
    
    def assertResponseStatus(self, response, assert_status):
        """Asserts a specific status code on the response"""
        self.assertEqual(response.status, assert_status,
            'Status code %s is not %s, as expected)\n\n%s' % 
            (response.status, assert_status, response.body))
    
class RestfulTestCase(HttpTestCase):
    """Performs restful HTTP request testing"""

    def restful_request(self, headers={}, json=None, xml=None, **kwargs):
        """Encodes and decodes (JSON & XML) HTTP requests and responses.
        
        Dynamically encodes json or xml request body if one is provided.

        WARNING: Existing Content-Type header will be overwritten.
        WARNING: If both json and xml are provided, the xml is ignored.
        WARNING: If either json or xml AND a body is provided, the body is
                 ignored.
        
        Dynamically returns 'json' or 'xml' attribute based on the detected
        response type, and fails the current test case if unsuccessful.
        
        response.json: standard python dictionary
        response.xml: xml.etree.ElementTree
        """
        
        # Attempt to encode JSON and XML automatically, if requested
        if json:
            body = self._encodeJson(json)
            headers['Content-Type'] = 'application/json'
        elif xml:
            body = self._encodeXml(xml)
            headers['Content-Type'] = 'application/xml'
        else:
            body = kwargs.get('body')
        
        # Perform the HTTP request/response
        response = self.request(headers=headers, body=body, **kwargs)
        
        # Attempt to parse JSON and XML automatically, if detected
        response = self._decodeResponseBody(response)
        
        # Contains the decoded response json/xml, etc
        return response
    
    def _encodeJson(self, data):
        """Returns a JSON-encoded string of the given python dictionary"""
        try:
            import json
            return json.dumps(data)
        except Exception as e:
            self.fail(e)
    
    def _encodeXml(self, data):
        """Returns an XML-encoded string of the given python dictionary"""
        self.fail('XML encoding is not yet supported by this framework')
    
    def _decodeResponseBody(self, response):
        """Detects response body type, and attempts to decode it"""
        if 'application/json' in response.getheader('Content-Type'):
            response.json = self._decodeJson(response.body)
        elif 'application/xml' in response.getheader('Content-Type'):
            response.xml = self._decodeXml(response.body)
        return response
    
    def _decodeJson(self, json_str):
        """Returns a dict of the given JSON string"""
        try:
            import json
            return json.loads(json_str)
        except Exception as e:
            self.fail(e)
    
    def _decodeXml(self, xml_str):
        """Returns an ElementTree of the given XML string"""
        try:
            import xml.etree.ElementTree
            return xml.etree.ElementTree.fromstring(xml_str)
        except Exception as e:
            self.fail(e)

class KeystoneTestCase(RestfulTestCase):
    """Perform generic HTTP request against Keystone APIs"""
    service_token = None
    
    admin_token = None
    admin_credentials = {
        'passwordCredentials':{
            'username': 'admin',
            'password': 'secrete',
        }
    }
    
    def setUp(self):
        """Prepare keystone for system tests"""
        # Authenticate as admin user to establish admin_token
        r = self.admin_request(method='POST', path='/tokens',
            json=self.admin_credentials)
        self.admin_token = r.json['auth']['token']['id']
    
    def service_request(self, path='', port=5000, headers={}, **kwargs):
        """Returns a request to the service API"""
        
        path = KeystoneTestCase._prepend_path(path)
        
        if self.service_token:
            headers['X-Auth-Token'] = self.service_token
        
        return self.restful_request(port=port, path=path, headers=headers,
            **kwargs)
    
    def admin_request(self, path='', port=5001, headers={}, **kwargs):
        """Returns a request to the admin API"""
        
        path = KeystoneTestCase._prepend_path(path)
        
        if self.admin_token:
            headers['X-Auth-Token'] = self.admin_token
        
        return self.restful_request(port=port, path=path, headers=headers, **kwargs)
    
    @staticmethod
    def _prepend_path(path):
        """Prepend the given path with the API version"""
        return '/v2.0' + str(path)
    
    @staticmethod
    def _uuid():
        """Generate and return a unique identifier"""
        import uuid
        return str(uuid.uuid4())
