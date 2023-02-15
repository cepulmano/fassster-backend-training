from tornado import autoreload
import tornado.httpserver
import tornado.ioloop
import tornado.web
import motor.motor_tornado

from decouple import config
import pandas as pd
import pymongo
import os

connectionURI = "mongodb://{}:{}@{}:{}/?authSource={}".format(config('MONGODB_USER'),config('MONGODB_PASSWORD'),config('MONGODB_HOST'),27017,config('MONGODB_AUTH'))
MONGO_DB = motor.motor_tornado.MotorClient(connectionURI)[config('MONGODB_DB')]
settings = {"debug": True, "fassster_mongodb": MONGO_DB}

class WebAppHandler(tornado.web.RequestHandler):
	async def get(self):
		location = self.get_argument("location", default=None,strip=False)
		as_of_date = self.get_argument("as_of_date", default=None,strip=False)

		# Look for processed output
		db = self.settings['fassster_mongodb']
		test = db.test
		cursor = test.find(
			{"location": location, "as_of_date": as_of_date},
			{"_id":0}
		).sort([("as_of_date", -1)]).limit(1)
		output = {}
		for document in await cursor.to_list(length=200):
			output = document

		self.set_header("content-type","application/json")
		if "location" in output.keys():
			print("Document found")
			self.write({"success": True, "data": output})
		else:
			# Load parquet file for processing
			df = pd.read_parquet(config('LINELIST'))
			filtered = df.loc[(df["regionPSGC"] == location) & (df["Report_Date"] == as_of_date)]
			if( len(df) > 0):
				# Format output
				output = {
					"location": location,
					"as_of_date": as_of_date,
					"total_cases": len(filtered)
				}

				# Insert Processed output to MongoDB
				filterParams = { "location": location,"as_of_date": as_of_date }
				updateOperator = {"$set": output}
				test.update_one(filterParams,updateOperator,upsert=True)

				self.write({"success": True, "data": output})
			else:
				self.write({"success": False, "message": "Error encounter in fetching the data."})

application = tornado.web.Application([
	(r"/fetch", WebAppHandler),
], **settings)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7777))
    tornado.httpserver.HTTPServer(application, max_buffer_size=10485760000)
    application.listen(port)
    ioloop = tornado.ioloop.IOLoop.instance()
    ioloop.start()