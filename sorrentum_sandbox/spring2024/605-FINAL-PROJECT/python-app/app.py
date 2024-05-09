from http.server import BaseHTTPRequestHandler, HTTPServer

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Hello, World from a simple Python HTTP server!")

if __name__ == '__main__':
    server_address = ('', 80)  # Serve on all addresses, port 80
    httpd = HTTPServer(server_address, SimpleHTTPRequestHandler)
    print("Server started on port 80...")
    httpd.serve_forever()
