import json
import re
import os

import SimpleHTTPServer
import SocketServer
from threading import Thread

PORT = 8997


class MockOdcHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_HEAD(self):
        self.send_response(405)  # not supported

    def do_GET(self):
        # test name is the first bit of the URL and makes CKAN behave
        # differently in some way.
        # Its value is recorded and then removed from the path
        print 'GET'
        self.test_name = None
        test_name_match = re.match('^/([^/]+)/', self.path)
        if test_name_match:
            self.test_name = test_name_match.groups()[0]
            self.path = re.sub('^/([^/]+)/', '/', self.path)

        # ignore paging (for now)
        if self.path.endswith('?page=2'):
            self.path = self.path.replace('?page=2', '')

        if self.path == '/data.rdf':
            if self.test_name == 'dataset1':
                return self.respond_rdf_file('odc_dataset1.rdf')
            elif self.test_name == 'developers-corner-theme':
                return self.respond_rdf_file('odc_developers_corner_theme.rdf')
            elif self.test_name == 'developers-corner-subject':
                return self.respond_rdf_file('odc_developers_corner_subject.rdf')
            elif self.test_name == 'developers-corner-folder':
                return self.respond_rdf_file('odc_developers_corner_folder.rdf')
            elif self.test_name == 'geography-theme':
                return self.respond_rdf_file('odc_geography_theme.rdf')
            elif self.test_name == 'geography-subject':
                return self.respond_rdf_file('odc_geography_subject.rdf')
            elif self.test_name == 'geography-folder':
                return self.respond_rdf_file('odc_geography_folder.rdf')
            else:
                raise NotImplementedError()

        # if we wanted to server a file from disk, then we'd call this:
        #return SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

        self.respond('Mock ODC doesnt recognize that call', status=400)

    def respond_rdf_file(self, sample_filename):
        self.path = sample_filename
        return SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

    def respond_json(self, content_dict, status=200):
        return self.respond(json.dumps(content_dict), status=status,
                            content_type='application/json')

    def respond(self, content, status=200, content_type='application/json'):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.end_headers()
        self.wfile.write(content)
        self.wfile.close()


def serve(port=PORT):
    '''Runs an ODC-alike app (over HTTP) that is used for harvesting tests'''

    # Choose the directory to serve files from
    os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          'samples'))

    class TestServer(SocketServer.TCPServer):
        allow_reuse_address = True

    httpd = TestServer(("", PORT), MockOdcHandler)

    print 'Serving test HTTP server at port', PORT

    httpd_thread = Thread(target=httpd.serve_forever)
    httpd_thread.setDaemon(True)
    httpd_thread.start()
