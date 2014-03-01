set -e

DIR="$( cd "$( dirname "$0" )" && pwd )"

if [ ! -d "$DIR/Twisted-13.2.0" ]; then
	wget "https://pypi.python.org/packages/source/T/Twisted/Twisted-13.2.0.tar.bz2#md5=83fe6c0c911cc1602dbffb036be0ba79" -P "$DIR"
	tar xfv "$DIR/Twisted-13.2.0.tar.bz2" -C "$DIR"
	rm -f "$DIR/Twisted-13.2.0.tar.bz2"
fi

PYTHONPATH="$DIR/Twisted-13.2.0/:$PYTHONPATH" python "$DIR/hlsproxy.py" "$@"
