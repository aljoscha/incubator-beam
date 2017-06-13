from flask import Flask
import os
import socket

app = Flask(__name__)

@app.route("/<string:query>")
def hello(query):
    return "Hello " + query

if __name__ == "__main__":
	app.run(host='0.0.0.0', port=80)
