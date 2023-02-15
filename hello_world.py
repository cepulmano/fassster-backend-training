import asyncio
import os
import tornado.web

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world!")

application = tornado.web.Application([
	(r"/", MainHandler),
])

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 80))
    tornado.httpserver.HTTPServer(application, max_buffer_size=10485760000)
    application.listen(port)
    ioloop = tornado.ioloop.IOLoop.instance()
    ioloop.start()