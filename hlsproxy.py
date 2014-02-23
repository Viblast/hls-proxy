#!/usr/bin/python

from sys import argv
from pprint import pformat
import argparse

from twisted.internet import defer
from twisted.internet.task import react
from twisted.web.client import Agent, RedirectAgent, readBody
from twisted.web.http_headers import Headers

class HlsItem:
	def __init__(self):
		self.dur = 0
		self.url = 0
	def __init__(self, dur, url):
		self.dur = dur
		self.url = url

class HlsPlaylist:
	def __init__(self):
		self.reset()
		
	def isValid(self):
		return len(self.errors) == 0
	
	def reset(self):
		self.version = 0
		self.targetDuration = 0
		self.items = []
		self.errors = []
		
	def fromStr(self, playlist):
		lines = playlist.split("\n")
		lines = filter(lambda x: x != "", lines)
		lines = map(lambda x: x.strip(), lines)
		
		if lines[0] != "#EXTM3U":
			self.errors.append("no #EXTM3U tag at the start of playlist")
			return
		lineIdx = 1
		while lineIdx < len(lines):
			line = lines[lineIdx]
			lineIdx += 1
			if line[0] == '#':
				keyValue = line.split(':')
				key = keyValue[0]
				value = keyValue[1]
				if key == "#EXT-X-VERSION":
					self.version = int(value)
				elif key == "#EXT-X-TARGETDURATION":
					self.targetDuration = int(value)
				elif key == "#EXT-X-MEDIA-SEQUENCE":
					self.mediaSequence = int(value)
				elif key == "#EXTINF":
					dur = int(value.split(',')[0]),
					url = lines[lineIdx]
					lineIdx += 1
					item = HlsItem(dur, url)
				else:
					print "Unknown tag: ", key
			else:
				print "Dangling playlit item: ", line

class HlsProxy:
	def __init__(self, reactor):
		self.reactor = reactor
		self.agent = RedirectAgent(Agent(reactor))
		self.clietPlaylist = HlsPlaylist()
		self.verbose = False
	
	def run(self, hlsPlaylist):
		self.finished = defer.Deferred()
		self.srvPlaylistUrl = hlsPlaylist
		self.refreshPlaylist()
		return self.finished

	def cbRequest(self, response):
		if self.verbose:
			print 'Response version:', response.version
			print 'Response code:', response.code
			print 'Response phrase:', response.phrase
			print 'Response headers:'
			print pformat(list(response.headers.getAllRawHeaders()))
		d = readBody(response)
		d.addCallback(self.cbBody)
		d.addErrback(lambda e: e.printTraceback())
		return d
		
	def cbBody(self, body):
		if self.verbose:
			print 'Response body:'
			print body
		playlist = HlsPlaylist()
		playlist.fromStr(body)
		self.onPlaylist(playlist)
	
	def onPlaylist(self, playlist):
		if playlist.isValid():
			pass
			# delete items before playlist.entries[0]
			#for item in playlist.items:
				# request item
			#update the playlist
			#wind playlist timer
			self.reactor.callLater(playlist.targetDuration, self.refreshPlaylist)
		else:
			print 'Invalide playlist. Retrying after default interval of 2s'
			self.reactor.callLater(2, self.retryPlaylist)
			
	def retryPlaylist(self):
		print 'Retrying playlist'
		self.refreshPlaylist()
	
	def refreshPlaylist(self):
		d = self.agent.request('GET', self.srvPlaylistUrl,
			Headers({'User-Agent': ['TODO Quick Time']}),
			None)
		d.addCallback(self.cbRequest)
		d.addErrback(lambda e: e.printTraceback())
		return d


def runProxy(reactor, args):
	proxy = HlsProxy(reactor)
	proxy.verbose = args.v
	d = proxy.run(args.hls_playlist)
	return d
	
def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("hls_playlist")
	parser.add_argument("-v", action="store_true")
	args = parser.parse_args()
	
	react(runProxy, [args])

if __name__ == "__main__":
	main()
