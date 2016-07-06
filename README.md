# Hls Proxy
hls-proxy is a tool that allows for easy downloading or mirroring of remote HLS streams.

## How to use

### Mirroring a remote live HLS stream
```shell
./start-proxy.sh http://server.com/live-stream.m3u8 -o .
```
The content of `http://server.com/live-stream.m3u8` will be downloaded to the output directory (specified by `-o`). The playlist will be constantly refreshed and when new content is available it will be also downloaded while old content will be removed. This will practically mirror the remote hls stream in the output directory (`-o`). The downloaded stream itself can be served using any HTTP server.

### Downloading a remote live HLS stream
```shell
./start-proxy.sh http://server.com/live-stream.m3u8 -o . -d
```
This will behave just as the above command except that old content will not be deleted effectively downloading the live stream in the output directory (`-o`). This is useful for downloading a long sample of a live stream that can latter be used for debugging and testing puposes.

### Downloaidng a remote VoD HLS steram
```shell
./start-proxy.sh http://server.com/vod-stream.m3u8 -o . -d
```
The VoD stream will be downloaded in the output directory (`-o`). In this case the download (`-d`) parameter is effectively ignored. In this example it is provided for clarity.

## Supported features
 * All HLS v3 features are supported including Live, VoD, ABR, and encrypted streams

## Dependencies
`hls-proxy` requires python-2.7, Twisted-13.2 and zope.interface. `start-proxy.sh` will download and setup Twisted. The only things that need to be installed manually are python and [zope.interface](https://pypi.python.org/pypi/zope.interface#download). Most linux distros provide these as packages. For detailed information on how to install them refer to your distro manual. Alternatively you can use `pip`.

## License
MIT
