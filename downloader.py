
from package import Mirror, PackageNotFoundError, Package
from pathlib import Path

import json
import argparse
import os
import concurrent.futures

parser = argparse.ArgumentParser( )
parser.add_argument( "packages", help="Directory containing packages.json" )

args = parser.parse_args( )

def get_deps( path: os.PathLike ):
    with open( path, "r" ) as f:
        package = json.load( f )

    deps = set()

    for dep, version in package.get( "dependencies", {} ).items( ):
        deps.add( dep + "@" + version )

    for dep, version in package.get( "devDependencies", {} ).items( ):
        deps.add( dep + "@" + version )

    return deps


first_class_deps = set()

for p in Path( args.packages ).iterdir( ):
    if p.is_file( ):
        first_class_deps.update( get_deps( p ) )

m = Mirror( "https://registry.npmjs.org/", cache_dir="cache" )

roots = set()

for dep in first_class_deps:
    roots.update( m.get_matching_packages( dep ) )

all_deps    = set( )
all_missing = set( )

for p in sorted( roots ):
    print( p )

    try:
        deps = m.strat2( p )
        all_deps.update( deps )
    except PackageNotFoundError:
        print( "Missing deps" )

download_size = 0

executor = concurrent.futures.ThreadPoolExecutor( max_workers=16 )
jobs     = []
for dep in all_deps:
    jobs.append( executor.submit( dep.populate ) )

concurrent.futures.wait( jobs, return_when=concurrent.futures.ALL_COMPLETED )

for dep in sorted( list( all_deps ) ):
    download_size += dep.download_size

print( "Download size: %d" % download_size )

def download( package: Package ):
    if os.path.exists( "data/%s" % package.hash ):
        return

    print( "Downloading %r" % package )

    with package.data_stream( ) as stream:
        with open( "data/%s" % package.hash, "wb" ) as f:
            while True:
                data = stream.read( 1024 * 1024 )
                if not data:
                    break
                f.write( data )

jobs = []
for dep in sorted( list( all_deps ) ):
    jobs.append( executor.submit( download, dep ) )

concurrent.futures.wait( jobs, return_when=concurrent.futures.ALL_COMPLETED )
