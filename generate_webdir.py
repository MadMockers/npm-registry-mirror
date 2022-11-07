#!/usr/bin/env python3

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

    existing = {}

    if ( Path( "repo" ) / urllib.parse.quote( original_manifest[ "name" ], safe='@' ) ).exists( ):
        try:
            with open( Path( "repo" ) / urllib.parse.quote( original_manifest[ "name" ], safe='@' ), "r" ) as f:
                existing = json.load( f ).get( "versions", {} )
        except:
            pass

    modified = False
    remove = set() # type: Set[str]
    for version, package in original_manifest[ "versions" ].items( ):
        package_hash = package[ "dist" ][ "shasum" ]
        if not ( Path( "data" ) / package_hash ).exists( ):
            remove.add( version )
            if version in existing:
                print( "%s: Removing a version that was in existing" % original_manifest[ "name" ] )
                modified = True

        else:
            new_url = urllib.parse.urljoin( url_root, "data/" + package_hash )
            package[ "dist" ][ "tarball" ] = new_url

            if version not in existing or new_url != existing[ version ][ "dist" ][ "tarball" ]:
                if version in existing:
                    print( "%s: Updating %s to %s" % ( original_manifest[ "name" ], existing[ version ][ "dist" ][ "tarball" ], new_url ) )
                modified = True

    if not modified:
        return

    for version in remove:
        del original_manifest[ "versions" ][ version ]


    if len( original_manifest[ "versions" ] ) > 0:
        print( "Saving %s" % original_manifest[ "name" ] )
        with open( Path( "repo" ) / urllib.parse.quote( original_manifest[ "name" ], safe='@' ), "w" ) as f:
            json.dump( original_manifest, f )

        if "/" in original_manifest[ "name" ]:
            with_slash = urllib.parse.quote( original_manifest[ "name" ], safe='@/' )

            if "/.." not in with_slash:
                ( Path( "repo" ) / with_slash ).parent.mkdir( parents=True, exist_ok=True )

                with open( Path( "repo" ) / with_slash, "w" ) as f:
                    json.dump( original_manifest, f )
    else:
        print( "Ignoring empty %s" % original_manifest[ "name" ] )


for filename in os.listdir( "cache/" ):
    no_ext, ext = os.path.splitext( filename )

    if os.path.exists( os.path.join( "cache", no_ext + ".metadata.json" ) ):
        generate_manifest( args.url_root, str( Path( "cache" ) / filename ) )
