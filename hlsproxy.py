#!/usr/bin/python

import os, copy
from sys import argv
from pprint import pformat
import argparse

from twisted.internet import defer
from twisted.internet.task import react
from twisted.web.client import HTTPConnectionPool
from twisted.web.client import Agent, RedirectAgent, readBody
from twisted.web.http_headers import Headers

class HlsItem:
	def __init__(self):
		self.dur = 0
		self.relativeUrl = ""
		self.absoluteUrl = ""
		self.mediaSequence = 0
	def __init__(self, dur, relativeUrl, absoluteUrl, mediaSequence):
		self.dur = dur
		self.relativeUrl = relativeUrl
		self.absoluteUrl = absoluteUrl
		self.mediaSequence = mediaSequence

class HlsPlaylist:
	def __init__(self):
		self.reset()
		
	def isValid(self):
		return len(self.errors) == 0
	
	def reset(self):
		self.version = 0
		self.targetDuration = 0
		self.mediaSequence = 0
		self.items = []
		self.errors = []
	
	def getItem(self, mediaSequence):
		idx = mediaSequence - self.mediaSequence
		if idx >= 0 and idx<len(self.items):
			return self.items[idx]
		else:
			return None
		
	def fromStr(self, playlist, playlistUrl):
		absoluteUrlBase = playlistUrl[:playlistUrl.rfind('/')+1]
		
		lines = playlist.split("\n")
		lines = filter(lambda x: x != "", lines)
		lines = map(lambda x: x.strip(), lines)
		
		if len(lines) == 0:
			self.errors.append("Empty playlist")
			return
		if lines[0] != "#EXTM3U":
			self.errors.append("no #EXTM3U tag at the start of playlist")
			return
		lineIdx = 1 
		msIter = 0
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
					dur = float(value.split(',')[0])
					url = lines[lineIdx]
					lineIdx += 1
					item = HlsItem(dur, url, absoluteUrlBase+url, self.mediaSequence+msIter)
					msIter += 1
					self.items.append(item)
				else:
					print "Unknown tag: ", key
			else:
				print "Dangling playlit item: ", line
		if len(self.items) == 0:
			self.errors.append("No items in the playlist")
	
	def toStr(self):
		res = "#EXTM3U\n"
		res += "#EXT-X-VERSION:" + str(self.version) + "\n"
		res += "#EXT-X-TARGETDURATION:" + str(self.targetDuration) + "\n"
		res += "#EXT-X-MEDIA-SEQUENCE:" + str(self.mediaSequence) + "\n"
		for item in self.items:
			res += "#EXTINF:" + str(item.dur) + ",\n"
			res += item.relativeUrl + "\n"
		return res

class HlsProxy:
	def __init__(self, reactor):
		self.reactor = reactor
		pool = HTTPConnectionPool(reactor)
		pool.maxPersistentPerHost = 2
		pool.cachedConnectionTimeout = 600
		self.agent = RedirectAgent(Agent(reactor, pool=pool))
		self.clientPlaylist = HlsPlaylist()
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
		playlist.fromStr(body, self.srvPlaylistUrl)
		self.onPlaylist(playlist)
		
	def getClientFilename(self, item):
		return "stream" + str(item.mediaSequence) + ".ts"
	
	def onPlaylist(self, playlist):
		if playlist.isValid():
			#deline old files
			for item in self.clientPlaylist.items:
				if playlist.getItem(item.mediaSequence) is None:
					os.unlink(self.getClientFilename(item))
			#request new ones
			for item in playlist.items:
				if self.clientPlaylist.getItem(item.mediaSequence) is None:
					self.requestFragment(item)
			#update the playlist
			self.clientPlaylist = playlist
			self.refreshClientPlaylist()
			#wind playlist timer
			self.reactor.callLater(playlist.targetDuration, self.refreshPlaylist)
		else:
			print 'The following errors where encountered while parsing the server playlist:'
			for err in playlist.errors:
				print '\t', err
			print 'Invalide playlist. Retrying after default interval of 2s'
			self.reactor.callLater(2, self.retryPlaylist)
			
	def writeFile(self, fileName, content):
		print 'cwd=', os.getcwd(), ' writing file', fileName 
		f = open(fileName, 'w')
		f.write(content)
		f.flush()
		os.fsync(f.fileno())
		f.close()
			
	def refreshClientPlaylist(self):
		playlist = self.clientPlaylist
		pl = HlsPlaylist()
		pl.version = playlist.version
		pl.targetDuration = playlist.targetDuration
		pl.mediaSequence = playlist.mediaSequence
		for item in playlist.items:
			itemFilename = self.getClientFilename(item)
			print "itemFilename=", itemFilename
			if os.path.isfile(itemFilename):
				ritem = copy.deepcopy(item)
				ritem.relativeUrl = itemFilename
				pl.items.append(ritem)
			else:
				break
		self.writeFile("stream.m3u8", pl.toStr())
	
	def retryPlaylist(self):
		print 'Retrying playlist'
		self.refreshPlaylist()
	
	def refreshPlaylist(self):
		d = self.agent.request('GET', self.srvPlaylistUrl,
			Headers({'User-Agent': ['AppleCoreMedia/1.0.0.13B42 (Macintosh; U; Intel Mac OS X 10_9_1; en_us)']}),
			None)
		d.addCallback(self.cbRequest)
		d.addErrback(self.onGetPlaylistError)
		return d

	def onGetPlaylistError(self, e):
		print "Error while getting the playlist: ", e
	        print "Retring after default interval of 2s"
		self.reactor.callLater(2, self.retryPlaylist)
	
	def cbFragment(self, response, item):
		if self.verbose:
			print 'Response version:', response.version
			print 'Response code:', response.code
			print 'Response phrase:', response.phrase
			print 'Response headers:'
			print pformat(list(response.headers.getAllRawHeaders()))
		d = readBody(response)
		thiz = self
		d.addCallback(lambda b: thiz.cbFragmentBody(b, item))
		d.addErrback(lambda e: e.printTraceback())
		return d
	
	def cbFragmentBody(self, body, item):
		if not(self.clientPlaylist.getItem(item.mediaSequence) is None):
			self.writeFile(self.getClientFilename(item), body)
		#else old request
		self.refreshClientPlaylist()
	
	def requestFragment(self, item):
		d = self.agent.request('GET', item.absoluteUrl,
			Headers({'User-Agent': ['AppleCoreMedia/1.0.0.13B42 (Macintosh; U; Intel Mac OS X 10_9_1; en_us)']}),
			None)
		thiz = self
		d.addCallback(lambda r: thiz.cbFragment(r, item))
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
