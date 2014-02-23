#!/usr/bin/python

from sys import argv
from pprint import pformat
import argparse

from twisted.internet.task import react
from twisted.web.client import Agent, RedirectAgent, readBody
from twisted.web.http_headers import Headers

class HlsProxy:
	def __init__(self, reactor):
		self.agent = RedirectAgent(Agent(reactor))
		self.verbose = False
	def run(self, hlsPlaylist):
		d = self.agent.request('GET', hlsPlaylist,
			Headers({'User-Agent': ['TODO Quick Time']}),
			None)
		d.addCallback(cbRequest)
		return d

def cbRequest(response):
	print 'Response version:', response.version
	print 'Response code:', response.code
	print 'Response phrase:', response.phrase
	print 'Response headers:'
	print pformat(list(response.headers.getAllRawHeaders()))
	d = readBody(response)
	d.addCallback(cbBody)
	return d
	
def cbBody(body):
	print 'Response body:'
	print body

def runProxy(reactor, args):
	proxy = HlsProxy(reactor)
	d = proxy.run(args.hls_playlist)
	return d
	
def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("hls_playlist")
	args = parser.parse_args()
	
	react(runProxy, [args])

if __name__ == "__main__":
	main()
