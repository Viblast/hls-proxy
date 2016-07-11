#!/usr/bin/env python

import urlparse

import os, copy, errno
from sys import argv
from pprint import pformat
import argparse
import re

import subprocess

from twisted.internet import defer
from twisted.internet.task import react
from twisted.web.client import HTTPConnectionPool
from twisted.web.client import Agent, RedirectAgent, readBody
from twisted.web.http_headers import Headers

def make_p(dir):
    try:
        os.makedirs(dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(dir):
            pass
        else:
            raise

class HlsItem:
    def __init__(self):
        self.dur = 0
        self.relativeUrl = ""
        self.absoluteUrl = ""
        self.mediaSequence = 0

class HlsVarian:
    def __init__(self):
        self.programId=0
        self.bandwidth=0
        self.relativeUrl=""
        self.absoluteUrl=""
        self.codecs=""
        self.audio=""
        self.subtitles=""

class HlsMedia:
    def __init__(self):
        self.type = ""
        self.groupId = ""
        self.name = ""
        self.language = ""
        self.default = ""
        self.autoselect = ""
        self.forced = ""
        self.relativeUrl = ""
        self.absoluteUrl=""

class HlsEncryption:
    def __init__(self):
        self.method=""
        self.uri=""


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
        self.variants = []
        self.medias = []
        self.errors = []
        self.encryption = None

    def getItem(self, mediaSequence):
        idx = mediaSequence - self.mediaSequence
        if idx >= 0 and idx<len(self.items):
            return self.items[idx]
        else:
            return None

    def splitInTwo(self, line, delimiter):
        delimiterIndex = line.find(delimiter)
        return [line[0:delimiterIndex], line[delimiterIndex+1:]]

    def fromStr(self, playlist, playlistUrl):
        self.absoluteUrlBase = playlistUrl[:playlistUrl.rfind('/')+1]

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
                keyValue = self.splitInTwo(line, ':')
                key = keyValue[0]
                value = keyValue[1] if len(keyValue) >= 2 else None
                if key == "#EXT-X-VERSION":
                    self.version = int(value)
                elif key == "#EXT-X-TARGETDURATION":
                    self.targetDuration = int(value)
                elif key == "#EXT-X-MEDIA-SEQUENCE":
                    self.mediaSequence = int(value)
                elif key == "#EXT-X-KEY":
                    self.handleEncryptionInfo(value)
                elif key == "#EXT-X-STREAM-INF":
                    self.handleVariant(value, lines[lineIdx])
                    lineIdx += 1
                elif key == "#EXT-X-MEDIA":
                    self.handleMedia(value)
                elif key == "#EXTINF":
                    dur = float(value.split(',')[0])
                    url = lines[lineIdx]
                    lineIdx += 1
                    item = HlsItem()
                    item.dur = dur
                    self.fillUrls(item, url)
                    item.mediaSequence = self.mediaSequence + msIter;
                    msIter += 1

                    self.items.append(item)
                else:
                    print "Unknown tag: ", key
            else:
                print "Dangling playlit item: ", line
        if len(self.items) == 0 and len(self.variants) == 0:
            self.errors.append("No items in the playlist")

    def handleEncryptionInfo(self, argStr):
        encryption = HlsEncryption()
        self.encryption = encryption
        keyValString = argStr.split(',')
        for keyValStr in keyValString:
            keyVal = self.splitInTwo(keyValStr, '=')
            if keyVal[0] == "METHOD":
                encryption.method = keyVal[1]
            elif keyVal[0] == "URI":
                encryption.uri = urlparse.urljoin(self.absoluteUrlBase, keyVal[1].strip('"'))

    def handleVariant(self, argStr, playlistUrl):
        variant = HlsVarian()
        self.variants.append(variant)
        kv = dict(re.findall(r'([\w-]+)=(".*?"|\d+)', argStr))
        for key, val in kv.iteritems():
            if key == "PROGRAM-ID":
                variant.programId = int(val)
            elif key == "BANDWIDTH":
                variant.bandwidth = int(val)
            elif key == "CODECS":
                variant.codecs = val
            elif key == "AUDIO":
                variant.audio = val
            elif key  == "SUBTITLES":
                variant.subtitles = val
        self.fillUrls(variant, playlistUrl)

    def handleMedia(self, argStr):
        media = HlsMedia()
        self.medias.append(media)
        kv = dict(re.findall(r'([\w-]+)=(".*?"|\d+|\w+)', argStr))
        uri = ''
        for key, val in kv.iteritems():
            val = val.strip('"')
            if key == "TYPE":
                media.type = val
            elif key == "GROUP-ID":
                media.groupId = val
            elif key == "NAME":
                media.name = val
            elif key == "LANGUAGE":
                media.language = val
            elif key == "DEFAULT":
                media.default = val
            elif key == "FORCED":
                media.forced = val
            elif key == "AUTOSELECT":
                media.autoselect = val
            elif key == "URI":
                uri = val
        self.fillUrls(media, uri)

    def fillUrls(self, item, playlistUrl):
        item.relativeUrl = playlistUrl
        item.absoluteUrl = urlparse.urljoin(self.absoluteUrlBase, playlistUrl)
        #if playlistUrl.find('://') > 0:
        #    item.absoluteUrl = playlistUrl
        #else:
        #    item.absoluteUrl = self.absoluteUrlBase + playlistUrl

    def toStr(self):
        if not self.variants:
            return self.toStrNormal()
        else:
            return self.toStrVariant()

    def toStrNormal(self):
        res = "#EXTM3U\n"
        res += "#EXT-X-VERSION:" + str(self.version) + "\n"
        res += "#EXT-X-TARGETDURATION:" + str(self.targetDuration) + "\n"
        res += "#EXT-X-MEDIA-SEQUENCE:" + str(self.mediaSequence) + "\n"
        if self.encryption != None:
            res += "#EXT-X-KEY:METHOD=" + self.encryption.method + ",URI=" + self.encryption.uri + '\n'
        for item in self.items:
            res += "#EXTINF:" + str(item.dur) + ",\n"
            res += item.relativeUrl + "\n"
        return res

    def toStrVariant(self):
        res = "#EXTM3U\n"
        res += ("#EXT-X-VERSION:" + str(self.version) + "\n") if self.version else ''
        for media in self.medias:
            res += '#EXT-X-MEDIA:TYPE={},GROUP-ID="{}",NAME="{}"'.format(media.type, media.groupId, media.name)
            res += ',DEFAULT={}'.format(media.default) if media.default else ''
            res += ',FORCED={}'.format(media.forced) if media.forced else ''
            res += ',LANGUAGE="{}"'.format(media.language) if media.language else ''
            res += ',AUTOSELECT={}'.format(media.autoselect) if media.autoselect else ''
            res += ',URI="{}"'.format(media.absoluteUrl) if media.absoluteUrl else ''
            res += '\n'
        for variant in self.variants:
            res += "#EXT-X-STREAM-INF:PROGRAM-ID={},BANDWIDTH={}".format(variant.programId, variant.bandwidth)
            if variant.codecs:
                res += ",CODECS={}".format(variant.codecs)
            if variant.audio:
                res += ",AUDIO={}".format(variant.audio)
            if variant.subtitles:
                res += ",SUBTITLES={}".format(variant.subtitles)
            res += "\n"
            res += variant.absoluteUrl + "\n"
        return res

class HttpReqQ:
    def __init__(self, agent, reactor):
        self.agent = agent
        self.reactor = reactor
        self.busy = False
        self.q = []

    class Req:
        def __init__(self, method, url, headers, body):
            self.method = method
            self.url = url
            self.headers = headers
            self.body = body
            self.d = defer.Deferred()

    def request(self, method, url, headers, body):
        req = HttpReqQ.Req(method, url, headers, body)
        self.q.append(req)
        self._processQ()
        return req.d

    def readBody(self, httpHeader):
        self.busy = True
        dRes = defer.Deferred()
        d = readBody(httpHeader)
        print("Reading body")
        d.addCallback(lambda body: self._readBodyCallback(dRes, body))
        d.addErrback(lambda err: self._readBodyErrback(dRes, err))
        return dRes

    def _reqCallback(self, req, res):
        print("Body read")
        self.busy = False
        req.d.callback(res)
        self._processQ()

    def _reqErrback(self, req, res):
        self.busy = False
        req.d.errback(res)
        self._processQ()

    def _readBodyCallback(self, dRes, body):
        self.busy = False
        dRes.callback(body)
        self._processQ()

    def _readBodyErrback(self, dRes, err):
        self.busy = False
        dRes.errback(err)
        self._processQ()

    def _processQ(self):
        if not(self.busy) and len(self.q) > 0:
            print("Processing a new request from the queue")
            req = self.q.pop(0)
            dAdapter = self.agent.request(req.method,
                              req.url,
                              req.headers,
                              req.body)
            dAdapter.addCallback(lambda res: self._reqCallback(req, res))
            dAdapter.addErrback(lambda res: self._reqErrback(req, res))
            self.busy = True
            #set a 3 min timeout for all request. If unsuccessfull then call the errback
            timeoutCall = self.reactor.callLater(3*60, dAdapter.cancel)
            def completed(passthrough):
                if timeoutCall.active():
                    timeoutCall.cancel()
                return passthrough
            dAdapter.addBoth(completed)

class HlsProxy:
    def __init__(self, reactor):
        self.reactor = reactor
        pool = HTTPConnectionPool(reactor, persistent=True)
        pool.maxPersistentPerHost = 1
        pool.cachedConnectionTimeout = 600
        self.agent = RedirectAgent(Agent(reactor, pool=pool))
        self.reqQ = HttpReqQ(self.agent, self.reactor)
        self.clientPlaylist = HlsPlaylist()
        self.verbose = False
        self.download = False
        self.outDir = ""
        self.encryptionHandled=False

        # required for the dump durations functionality
        self.dur_dump_file = None
        self.dur_avproble_acc = 0
        self.dur_vt_acc = 0
        self.dur_playlist_acc = 0


    def setOutDir(self, outDir):
        outDir = outDir.strip()
        if len(outDir) > 0:
            self.outDir = outDir + '/'

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
        d = self.reqQ.readBody(response)
        d.addCallback(self.cbBody)
        d.addErrback(self.onGetPlaylistError)
        return d

    def cbBody(self, body):
        if self.verbose:
            print 'Response body:'
            print body
        playlist = HlsPlaylist()
        playlist.fromStr(body, self.srvPlaylistUrl)
        self._clientPlaylistText = body
        self.onPlaylist(playlist)

    def getSegmentFilename(self, item):
        return self.outDir + self.getSegmentRelativeUrl(item)

    def getSegmentRelativeUrl(self, item):
        return "stream" + str(item.mediaSequence) + ".ts"

    def getClientPlaylist(self):
        return self.outDir + "stream.m3u8"

    def get_individial_client_playlist(self, media_sequence):
        return self.outDir + "stream." + str(media_sequence) + ".m3u8"

    def onPlaylist(self, playlist):
        if playlist.isValid():
            self.onValidPlaylist(playlist)
        else:
            print 'The following errors where encountered while parsing the server playlist:'
            for err in playlist.errors:
                print '\t', err
            print 'Invalide playlist. Retrying after default interval of 2s'
            self.reactor.callLater(2, self.retryPlaylist)

    def onValidPlaylist(self, playlist):
        if playlist.encryption != None and not self.encryptionHandled:
            self.encryptionHandled = True
            if playlist.encryption.method in ['AES-128', 'SAMPLE-AES'] and playlist.encryption.uri != '':
                self.requestResource(playlist.encryption.uri, "key")
            else:
                print 'Unsupported encryption method ', playlist.encryption.method, 'uri', playlist.encryption.uri


        if len(playlist.variants) == 0:
            self.onSegmentPlaylist(playlist)
        else:
            self.onVariantPlaylist(playlist)

    def onSegmentPlaylist(self, playlist):
        #deline old files
        if not(self.download):
            for item in self.clientPlaylist.items:
                if playlist.getItem(item.mediaSequence) is None:
                    try:
                        os.unlink(self.getSegmentFilename(item))
                    except:
                        print "Warning. Cannot remove fragment ", self.getSegmentFilename(item), ". Probably it wasn't downloaded in time."
        #request new ones
        for item in playlist.items:
            if self.clientPlaylist.getItem(item.mediaSequence) is None:
                self.requestFragment(item)
        #update the playlist
        self.clientPlaylist = playlist
        self.refreshClientPlaylist()
        #wind playlist timer
        self.reactor.callLater(playlist.targetDuration, self.refreshPlaylist)

    def onVariantPlaylist(self, playlist):
        print "Found variant playlist."
        masterPlaylist = HlsPlaylist()
        masterPlaylist.version = playlist.version

        for variant in playlist.variants:
            subOutDir = self.outDir + str(variant.bandwidth)
            print "Starting a sub hls-proxy for channel with bandwith ", variant.bandwidth, " in directory ", subOutDir
            make_p(subOutDir)
            
            masterVariant = copy.deepcopy(variant)
            masterPlaylist.variants.append(masterVariant)
            masterVariant.absoluteUrl = str(variant.bandwidth) + "/stream.m3u8"
            
            self.start_subproxy(subOutDir, variant.absoluteUrl)

        for imedia, media in enumerate(playlist.medias):
            # imedia is appended just in case greoup, name, language turn out to be the same after sanitization
            mediaRelative = re.sub(r'[^\w-]', '', '{}-{}-{}-{}'.format(media.groupId, media.name, media.language, imedia))
            relativePath = os.path.join(media.type, mediaRelative)
            subOutDir = os.path.join(self.outDir, relativePath)
            print "Starting a sub hls-proxy for channel with bandwith ", media.type, " in directory ", subOutDir
            make_p(subOutDir)
            
            proxiedMedia = copy.deepcopy(media)
            masterPlaylist.medias.append(proxiedMedia)
            proxiedMedia.absoluteUrl = os.path.join(relativePath, "stream.m3u8")
            
            if media.relativeUrl:
                # EXT-X-MEDIA URI is optional so it's possible to have a media wihtout relativeUrl
                self.start_subproxy(subOutDir, media.absoluteUrl)

        self.writeFile(self.getClientPlaylist(), masterPlaylist.toStr())
        
    def start_subproxy(self, subOutDir, hlsUrl):
        subProxy = HlsProxy(self.reactor)
        subProxy.verbose = self.verbose
        subProxy.download = self.download
        subProxy.referer = self.referer
        subProxy.dump_durations = self.dump_durations
        subProxy.save_individual_playlists = self.save_individual_playlists
        subProxy.setOutDir(subOutDir)
        d = subProxy.run(hlsUrl)
        #TODO add the deffered to self.finised somehow
        return subProxy

    def writeFile(self, filename, content):
        print 'cwd=', os.getcwd(), ' writing file', filename
        f = open(filename, 'w')
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
        f.close()

    def dump_duration(self, filename, item):
        if not self.dump_durations: return
        format = '{filename: <30} {avp_dur: <12} {m3u8_dur: <10} {vt_dur: <12} {avp_m3u8_diff: <10} {vt_m3u8_diff: <10}   {avp_acc: <12} {m3u8_acc: <12} {vt_acc: <12} {avp_m3u8_acc_diff: <10} {vt_m3u8_acc_diff: <10}\n'
        if not self.dur_dump_file:
            self.dur_dump_file = open(self.outDir + 'duration-dump', 'wt')
            self.dur_dump_file.write(format.format(filename='FILENAME', avp_dur='AVPROBE DUR', m3u8_dur='M3U8 DUR', avp_m3u8_diff='AVP - M3U8',
                                    vt_dur='VT DUR', vt_m3u8_diff='VT - M3U8',
                                    avp_acc='AVPROBE ACC', m3u8_acc='M3U8 ACC', avp_m3u8_acc_diff='AVP-M3U8 ACC',
                                    vt_acc='VT ACC', vt_m3u8_acc_diff='VT-M3U8 ACC'))
        avprobe_duration = subprocess.check_output('avprobe -loglevel quiet -show_format_entry duration "{}"'.format(filename), shell=True).strip()
        vt_duration = subprocess.check_output('videotools -f ts -op duration "{}"'.format(filename), shell=True).strip()
        self.dur_avproble_acc += float(avprobe_duration)
        self.dur_vt_acc += float(vt_duration)
        self.dur_playlist_acc += float(item.dur)
        self.dur_dump_file.write(format.format(filename=filename, avp_dur=avprobe_duration, m3u8_dur=item.dur, avp_m3u8_diff=float(avprobe_duration) - float(item.dur),
                                vt_dur=vt_duration, vt_m3u8_diff=float(vt_duration) - float(item.dur),
                                avp_acc=self.dur_avproble_acc, m3u8_acc=self.dur_playlist_acc, avp_m3u8_acc_diff=self.dur_avproble_acc - self.dur_playlist_acc,
                                vt_acc=self.dur_vt_acc, vt_m3u8_acc_diff=self.dur_vt_acc - self.dur_playlist_acc))
        self.dur_dump_file.flush()

    def refreshClientPlaylist(self):
        playlist = self.clientPlaylist
        pl = HlsPlaylist()
        pl.version = playlist.version
        pl.targetDuration = playlist.targetDuration
        pl.mediaSequence = playlist.mediaSequence
        if playlist.encryption != None:
            pl.encryption = HlsEncryption()
            pl.encryption.method = playlist.encryption.method
            pl.encryption.uri = 'key'
        for item in playlist.items:
            itemFilename = self.getSegmentFilename(item)
            if os.path.isfile(itemFilename):
                ritem = copy.deepcopy(item)
                ritem.relativeUrl = self.getSegmentRelativeUrl(item)
                pl.items.append(ritem)
            else:
                print "Stopping playlist generation on itemFilename=", itemFilename
                break
        self.writeFile(self.getClientPlaylist(), pl.toStr())
        if self.save_individual_playlists:
            individual_pl_fn = self.get_individial_client_playlist(pl.mediaSequence)
            self.writeFile(individual_pl_fn, self._clientPlaylistText)

    def retryPlaylist(self):
        print 'Retrying playlist'
        self.refreshPlaylist()

    def refreshPlaylist(self):
        print "Getting playlist from ", self.srvPlaylistUrl
        d = self.reqQ.request('GET', self.srvPlaylistUrl,
            Headers(self.httpHeaders()),
            None)
        d.addCallback(self.cbRequest)
        d.addErrback(self.onGetPlaylistError)
        return d

    def httpHeaders(self):
                headers = {'User-Agent': ['AppleCoreMedia/1.0.0.13B42 (Macintosh; U; Intel Mac OS X 10_9_1; en_us)']}
                if self.referer:
                        headers['Referer'] = [self.referer]
                return headers

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
        d = self.reqQ.readBody(response)
        thiz = self
        d.addCallback(lambda b: thiz.cbFragmentBody(b, item))
        d.addErrback(lambda e: e.printTraceback())
        return d

    def cbFragmentBody(self, body, item):
        if not(self.clientPlaylist.getItem(item.mediaSequence) is None):
            self.writeFile(self.getSegmentFilename(item), body)
            self.dump_duration(self.getSegmentFilename(item), item)
        #else old request
        self.refreshClientPlaylist()

    def requestFragment(self, item):
        print "Getting fragment from ", item.absoluteUrl
        d = self.reqQ.request('GET', item.absoluteUrl,
            Headers(self.httpHeaders()),
            None)
        thiz = self
        d.addCallback(lambda r: thiz.cbFragment(r, item))
        d.addErrback(lambda e: e.printTraceback())
        return d

    def requestResource(self, url, localFilename):
        print "Getting resource from ", url, " -> ", localFilename
        d = self.reqQ.request('GET', url, Headers(self.httpHeaders()), None)
        thiz = self
        d.addCallback(lambda r: thiz.cbRequestResource(r, localFilename))
        d.addErrback(lambda e: e.printTraceback())
        return d

    def cbRequestResource(self, response, localFilename):
        d = self.reqQ.readBody(response)
        thiz = self
        d.addCallback(lambda b: thiz.cbRequestResourceBody(b, localFilename))
        d.addErrback(lambda e: e.printTraceback())
        return d

    def cbRequestResourceBody(self, body, localFilename):
        self.writeFile(localFilename, body)


def runProxy(reactor, args):
    proxy = HlsProxy(reactor)
    proxy.verbose = args.v
    proxy.download = args.d
    proxy.referer = args.referer
    proxy.dump_durations = args.dump_durations
    proxy.save_individual_playlists = args.save_individual_playlists
    if not(args.o is None):
        proxy.setOutDir(args.o)
    d = proxy.run(args.hls_playlist)
    return d

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("hls_playlist")
    parser.add_argument("-v", action="store_true")
    parser.add_argument("-d", action="store_true")
    parser.add_argument("--dump-durations", action="store_true")
    parser.add_argument("--save-individual-playlists", action="store_true")
    parser.add_argument("--referer")
    parser.add_argument("-o");
    args = parser.parse_args()

    react(runProxy, [args])

if __name__ == "__main__":
    main()
