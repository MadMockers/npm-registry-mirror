
from typing import Set

import urllib.parse
import json
import os
import sys
import argparse
from pathlib import Path

parser = argparse.ArgumentParser( )
parser.add_argument( "url_root", help="Where the webdirectory will be served from" )

args = parser.parse_args( )


def generate_manifest( url_root: str, cache_file: str ):
    with open( cache_file, "r" ) as f:
        original_manifest = json.load( f )

    remove = set() # type: Set[str]
    for version, package in original_manifest[ "versions" ].items( ):
        package_hash = package[ "dist" ][ "shasum" ]
        if not ( Path( "data" ) / package_hash ).exists( ):
            remove.add( version )

        else:
            package[ "dist" ][ "tarball" ] = urllib.parse.urljoin( url_root, "data/" + package_hash )

    for version in remove:
        del original_manifest[ "versions" ][ version ]


    if len( original_manifest[ "versions" ] ) > 0:
        print( "Saving %s" % original_manifest[ "name" ] )
        with open( Path( "repo" ) / urllib.parse.quote( original_manifest[ "name" ], safe='' ), "w" ) as f:
            json.dump( original_manifest, f )
    else:
        print( "Ignoring empty %s" % original_manifest[ "name" ] )


for filename in os.listdir( "cache/" ):
    no_ext, ext = os.path.splitext( filename )

    if os.path.exists( os.path.join( "cache", no_ext + ".metadata.json" ) ):
        generate_manifest( args.url_root, str( Path( "cache" ) / filename ) )
