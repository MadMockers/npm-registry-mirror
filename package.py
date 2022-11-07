
from typing import Dict, Any, Iterable, NamedTuple, Set, Optional, Generator, Tuple, OrderedDict, List, Union, Awaitable, cast

from pathlib import Path

import copy
import asyncio
import os
import json
import datetime
import semver
import concurrent.futures
import threading
import requests
import urllib.parse
import contextlib

#from backports.datetime_fromisoformat import MonkeyPatch
#MonkeyPatch.patch_fromisoformat( )


class PackageNotFoundError( RuntimeError ):
    pass


class RequestsError( RuntimeError ):

    def __init__( self, response: requests.Response ):
        self.response = response


class Package:

    def __init__( self, manifest: "Manifest",
                        package:  Dict[str, Any] ):
        self._manifest      = manifest
        self._package       = package

        self._populate_lock = threading.Lock( )
        self._download_size = None

        self._cache_lock    = threading.Lock( )
        self._saved         = False
        self._loaded        = False
        self.load( )


    def save( self ):
        cache_dir = self.mirror.cache_dir

        if cache_dir is None:
            return

        with self._cache_lock:
            if not self._saved:
                hash = self._package[ "dist" ][ "shasum" ]

                metadata = {
                    "download_size": self._download_size,
                }

                with open( str( Path( cache_dir ) / ( hash + ".json" ) ), "w" ) as f:
                    json.dump( metadata, f )

                self._saved = True


    def load( self ):
        cache_dir = self.mirror.cache_dir

        if cache_dir is None:
            return

        with self._cache_lock:
            if self._loaded:
                return

            try:
                hash = self._package[ "dist" ][ "shasum" ]

                with open( str( Path( cache_dir ) / ( hash + ".json" ) ), "r" ) as f:
                    metadata = json.load( f )

                self._download_size = metadata.get( "download_size", None )

                self._saved  = True
                self._loaded = True

            except FileNotFoundError:
                pass


    def populate( self ):
        with self._populate_lock:
            if self._download_size is not None:
                return

            try:
                response = requests.head( self._package[ "dist" ][ "tarball" ], headers={ "Accept-Encoding": "identity" } )
                if response.status_code != 200:
                    raise RequestsError( response )

                self._download_size = int( response.headers[ "content-length" ] )

                self._populated = True

                self.save( )
            except Exception as e:
                raise RuntimeError( "Failed to populate %s" % self.name )


    @property
    def mirror( self ):
        return self._manifest.mirror


    @property
    def name( self ):
        return "%s@%s" % ( self._package[ "name" ], self._package[ "version" ] )


    @property
    def hash( self ):
        return self._package[ "dist" ][ "shasum" ]


    @property
    def dependencies( self, include_dev: bool = False ) -> Iterable[str]:
        dependencies = set( ) # type: Set[str]

        types = [ "dependencies", "peerDependencies" ]
        if include_dev:
            types.append( "devDependencies" )

        for dep_type in types:
            for dep, version in self._package.get( dep_type, {} ).items( ):
                dependencies.add( "%s@%s" % ( dep, version ) )

        return dependencies


    @property
    def download_size( self ):
        self.populate( )
        return self._download_size


    def data_stream( self ):
        response = requests.get( self._package[ "dist" ][ "tarball" ], stream=True )
        if response.status_code != 200:
            raise RequestsError( response )
        return response.raw


    def __repr__( self ):
        return self.name


    def __str__( self ):
        return self.__repr__( )


    def __lt__( self, other ):
        return self.name < other.name


    def __hash__( self ):
        return id( self )


    def __eq__( self, other ):
        return id( self ) == id( other )


class Manifest:

    def __init__( self, mirror: "Mirror", name: str ):
        self._name          = name
        self._mirror        = mirror
        self._url           = mirror.url + "/" + urllib.parse.quote( name, safe='' )
        self._last_update   = None
        self._manifest      = None # type: Optional[Dict[str, Any]]
        self._semvers       = {}   # type: Optional[Dict[semver.SemVer, Dict[str, Any]]]
        self._stale         = True
        self._etag          = None
        self._size          = None # type: Optional[int]

        self._range_memo    = {}   # type: Dict[str, str]

        self._package_cache_lock = threading.Lock( )
        self._package_cache      = {}   # type: Dict[str, Package]

        self._cache_lock = threading.Lock( )
        self._saved      = False
        self._loaded     = False

        self.load( )


    def save( self ):
        cache_dir = self.mirror.cache_dir

        if cache_dir is None:
            return

        with self._cache_lock:
            if not self._stale and not self._saved:
                name = urllib.parse.quote( self.name, safe='' )

                with open( str( Path( cache_dir ) / ( name + ".json" ) ), "w" ) as f:
                    json.dump( self._manifest, f )

                metadata = {
                    "etag":        self._etag,
                    "last_update": self._last_update.isoformat( ) if self._last_update else None,
                }

                with open( str( Path( cache_dir ) / ( name + ".metadata.json" ) ), "w" ) as f:
                    json.dump( metadata, f )

                self._saved = True


    def load( self ):
        cache_dir = self.mirror.cache_dir

        if cache_dir is None:
            return

        with self._cache_lock:
            if self._loaded:
                return

            try:
                name = urllib.parse.quote( self.name, safe='' )

                with open( str( Path( cache_dir ) / ( name + ".json" ) ), "r" ) as f:
                    self.__set_manifest( json.load( f ) )

                with open( str( Path( cache_dir ) / ( name + ".metadata.json" ) ), "r" ) as f:
                    metadata = json.load( f )

                self._etag  = metadata.get( "etag",        None )
                last_update = metadata.get( "last_update", None )

                if last_update:
                    self._last_update = datetime.datetime.fromisoformat( last_update )
                    diff = datetime.timedelta( minutes=30 )
                    if diff >= datetime.datetime.now( ) - self._last_update:
                        self._stale = False

                self._saved  = True
                self._loaded = True
            except FileNotFoundError:
                pass
            except:
                raise RuntimeError( "Failed to load %s" % self.name )


    def __set_manifest( self, manifest: Dict[str, Any] ):
        self._manifest = manifest
        self._semvers = { semver.SemVer( version, loose=False ): data for version, data in self._manifest[ "versions" ].items( ) }


    @property
    def name( self ):
        return self._name


    @property
    def mirror( self ):
        return self._mirror


    @property
    def etag( self ):
        return self._etag


    def exists( self ):
        if not self._stale:
            return True

        return requests.head( self._url ).status_code == 200


    @property
    def manifest( self ) -> Dict[str, Any]:
        if self._stale:
            headers = None

            if self._etag is not None and self._manifest is not None:
                headers = {}
                headers[ "if-none-match" ] = self._etag

            response = requests.get( self._url )

            updated = False

            if response.status_code != 304:
                if response.status_code != 200:
                    raise PackageNotFoundError( "Package %s does not exist" % self.name )

                self._last_update = datetime.datetime.now( )
                self._etag        = response.headers.get( "etag", None )
                self.__set_manifest( cast( Dict[str, Any], response.json( ) ) )
                self._saved       = False
                updated = True

            self._stale = False

            if updated:
                self.save( )

        if self._manifest is None:
            raise RuntimeError( "BUG: Manifest is None" )

        return self._manifest


    def _ensure_package( self, version_str: str ):
        with self._package_cache_lock:
            if version_str in self._package_cache:
                package = self._package_cache[ version_str ]
            else:
                package = Package( self, self.manifest[ "versions" ][ version_str ] )
                self._package_cache[ version_str ] = package

        return package


    @property
    def versions( self ) -> Generator[Package, None, None]:
        for version in self.manifest[ "versions" ].keys( ):
            yield self._ensure_package( version )


    def get_package( self, version_string: str = None ):
        if version_string is None:
            version_string = "*"

        if version_string in self._range_memo:
            return self._package_cache[ self._range_memo[ version_string ] ]

        versions = self.manifest[ "versions" ].keys( )
        max_sat  = semver.max_satisfying( versions, version_string, loose=False )

        if max_sat is None:
            raise PackageNotFoundError( "%s@%s not found" % ( self.name, version_string ) )

        package = self._ensure_package( max_sat )
        self._range_memo[ version_string ] = max_sat

        return package


    def get_matching_packages( self, version_string: str ):
        matching = []
        for version in self.manifest[ "versions" ]:
            if semver.satisfies( version, version_string ):
                matching.append( self._ensure_package( version ) )

        return matching


class CyclicDependency:

    def __init__( self, cycle: List["ResolveNode"] ):
        if len( cycle ) == 1:
            raise RuntimeError( "A cycle must have more than one member" )

        self.cycle = set( cycle )


    def __hash__( self ):
        h = 0

        for p in self.cycle:
            h ^= hash( p )

        return h


    def __eq__( self, other ):
        if not isinstance( other, CyclicDependency ):
            return False

        return self.cycle == other.cycle


    def __repr__( self ):
        return "[Cycle %r]" % ( self.cycle )


class ResolveNode:

    def __init__( self, package: Package ):
        self.package   = package
        self.cond      = asyncio.Condition( )
        self.resolving = False
        self.resolved  = False
        self.waiters   = {}    # type: Dict[int, OrderedDict[ResolveNode, None]]
        self.deps      = set() # type: Set[Union[CyclicDependency, Package]]
        #self.cycles    = set() # type: Set[CyclicDependency]


    def __hash__( self ):
        return hash( self.package )


    def __eq__( self, other ):
        if not isinstance( other, ResolveNode ):
            return False

        return self.package == other.package


    def __repr__( self ):
        return self.package.__repr__( )


class RecursiveResolver:

    def __init__( self ):
        self._loop          = asyncio.new_event_loop( )
        self._executor      = concurrent.futures.ThreadPoolExecutor( max_workers=5 )
        self._state_lock    = asyncio.Lock( )

        self._nodes         = {} # type: Dict[Package, ResolveNode]

    async def get_node( self, package: Package ):
        async with self._state_lock:
            if package in self._nodes:
                return self._nodes[ package ]
            else:
                return self._nodes.setdefault( package, ResolveNode( package ) )


    def build_cycle( self, cycle_trigger: ResolveNode,
                           path:          List[ResolveNode] ):
        # We've hit a cycle. Walk backwards to find the start of the cycle.
        # Since we're now operating on the start of the cycle, add the dependencies
        # directly, and then return None to indicate to our callers that they

        reverse_path = path[ ::-1 ]
        for idx, value in enumerate( reverse_path ):
            if value == cycle_trigger:
                cycle_members = [ x.package for x in reverse_path[ :idx+1 ] ]
                cycle = CyclicDependency( reverse_path[ :idx+1 ] )
                ret = set( ) # type: Set[Union[CyclicDependency, Package]]
                ret.add( cycle )
                ret.update( cycle_members )
                return ret

        raise RuntimeError( "BUG: Cycle detected but we aren't in path" )


    def check_for_deadlock( self, needle: ResolveNode, path: List[ResolveNode], visited: Optional[Set[ResolveNode]] = None ):
        if visited is None:
            visited = set( )

        current = path[ -1 ]

        if current in visited:
            return None
        visited = copy.copy( visited )
        visited.add( current )

        if current == needle:
            return [ needle ]

        if len( path ) > 1:
            cycle = self.check_for_deadlock( needle, path[ :-1 ], visited=visited )

            if cycle:
                return cycle + [ current ]

        for waiter_path in [ list( x ) for x in current.waiters.values( ) ]:
            if len( waiter_path ) > 1:
                cycle = self.check_for_deadlock( needle, waiter_path[ :-1 ], visited=visited )
                if cycle is not None:
                    return cycle + [ current ]

        return None


    def resolve_our_cycles( self, node: ResolveNode, path: List[ResolveNode] ):
        previous = path[ -2 ] if len( path ) > 1 else None

        our_cycles = set( )

        for dep in node.deps:
            if isinstance( dep, CyclicDependency ):
                if previous not in dep.cycle:
                    our_cycles.add( dep )

        deps = node.deps.difference( our_cycles )

        for c in our_cycles:
            for other in c.cycle:
                if other == node:
                    continue

                if c in other.deps:
                    other.deps.remove( c )
                    other.deps.update( deps )

        node.deps = deps


    def resolve( self, package: Package ):
        deps = self._loop.run_until_complete( self.resolve_recurse( package ) )

        for d in deps:
            if isinstance( d, CyclicDependency ):
                raise RuntimeError( "BUG: Dep returned a cyclic dependency: %r" % d )

        return deps


    async def resolve_recurse( self, package:  Package,
                                     visiting: Optional[OrderedDict[ResolveNode, None]] = None ) \
                                                -> Iterable[Union[CyclicDependency, Package]]:
        if visiting is None:
            visiting = OrderedDict()
        else:
            visiting = copy.copy( visiting )

        node = await self.get_node( package )

        if node in visiting:
            return self.build_cycle( node, list( visiting.keys( ) ) )

        visiting[ node ] = None
        path = list( visiting.keys( ) )

        async with node.cond:
            if node.resolving:
                cycle = self.check_for_deadlock( node, path[ :-1 ] )
                if cycle is not None:

                    cycle_dep = CyclicDependency( cycle )

                    ret = set( ) # type: Set[Union[CyclicDependency, Package]]
                    ret.add( cycle_dep )
                    ret.update( [ x.package for x in cycle ] )

                    return ret

                node.waiters[ id( visiting ) ] = visiting

                while node.resolving:
                    await node.cond.wait( )

                del node.waiters[ id( visiting ) ]

            if node.resolved:
                self.resolve_our_cycles( node, path )
                return node.deps

            else:
                node.resolving = True

        try:
            mirror = package.mirror
            get_tasks     = set() # type: Set[Awaitable[Package]]
            resolve_tasks = set() # type: Set[asyncio.Task[Iterable[Union[CyclicDependency, Package]]]]
            all_deps      = set() # type: Set[Union[CyclicDependency, Package]]

            active_tasks  = {}

            try:
                for dependency in package.dependencies:
                    task = self._loop.run_in_executor( self._executor, mirror.get_package, dependency )
                    get_tasks.add( task )

                while len( get_tasks ) > 0:
                    done, pending = await asyncio.wait( get_tasks, return_when=asyncio.FIRST_COMPLETED )
                    get_tasks = cast( Set[Awaitable[Package]], pending )

                    results = await asyncio.gather( *done, return_exceptions=True )

                    for result in results:
                        if isinstance( result, BaseException ):
                            raise result

                        dependency = result
                        all_deps.add( dependency )
                        resolve_task = asyncio.create_task( self.resolve_recurse( dependency, visiting=visiting ) )
                        resolve_tasks.add( resolve_task )
                        active_tasks[ resolve_task ] = dependency

                while len( resolve_tasks ) > 0:
                    done, pending = await asyncio.wait( resolve_tasks, return_when=asyncio.FIRST_COMPLETED )
                    resolve_tasks = pending

                    done = list( done )
                    results = await asyncio.gather( *done, return_exceptions=True )

                    for idx,result in enumerate( results ):
                        if isinstance( result, BaseException ):
                            raise result

                        for d in result:
                            if isinstance( d, CyclicDependency ):
                                if node not in d.cycle:
                                    raise RuntimeError( "BUG: Impossible cycle" )

                        all_deps.update( result )

            finally:
                for t in get_tasks:
                    t.cancel( ) # type: ignore
                for t in resolve_tasks:
                    t.cancel( )
                await asyncio.gather( *get_tasks, *resolve_tasks, return_exceptions=True )
            
            async with self._state_lock:

                node.deps.update( all_deps )
                self.resolve_our_cycles( node, path )
                node.resolved = True

        finally:
            async with node.cond:
                node.resolving = False
                node.cond.notify_all( )

        return node.deps

            
class Mirror:

    def __init__( self, url: str, cache_dir = None ):
        self._lock           = threading.Lock( )
        self._url            = url
        self._manifest_cache = {} # type: Dict[str, Manifest]
        self._cache_dir      = cache_dir
        self._resolver       = RecursiveResolver( )

        if cache_dir is not None:
            os.makedirs( cache_dir, exist_ok=True )

        self._deps = {}


    @property
    def cache_dir( self ):
        return self._cache_dir


    @property
    def url( self ):
        return self._url


    def get_manifest( self, name: str, populate: bool = True ):
        manifest = None # type: Optional[Manifest]

        with self._lock:
            if name in self._manifest_cache:
                manifest = self._manifest_cache[ name ]
                manifest.load( )

            if manifest is None:
                manifest = Manifest( self, name )
                self._manifest_cache[ name ] = manifest

        if not manifest.exists( ) and not populate:
            raise PackageNotFoundError( "Package %s does not exist" % name )

        if populate:
            # This will raise PackageNotFoundError if not found
            manifest.manifest

        return manifest


    def get_package( self, name:    str,
                           version: str = None ):
        version_split = name.rfind( "@" )
        if version_split > 0:
            version = name[ version_split+1: ]
            name    = name[ :version_split ]

        manifest = self.get_manifest( name )
        return manifest.get_package( version )


    def get_matching_packages( self, name:    str,
                                     version: str = None ):
        version_split = name.rfind( "@" )
        if version_split > 0:
            version = name[ version_split+1: ]
            name    = name[ :version_split ]

        manifest = self.get_manifest( name )
        return manifest.get_matching_packages( version )


    def strat2( self, package: Package, include_self=True ):
        deps = self._resolver.resolve( package )
        if include_self:
            deps.add( package )
        return deps


    def get_all_dependencies( self, package: Package, ignore_missing=False ) -> Tuple[Iterable[Package], Iterable[str]]:
        executor   = concurrent.futures.ThreadPoolExecutor( max_workers=16 )

        job_packages = {}
        jobs         = set()
        processed    = set() # type: Set[str]
        deps         = set() # type: Set[Package]
        missing      = []

        jobs.add( executor.submit( lambda: package ) )

        while len( jobs ) > 0:
            finished, pending = concurrent.futures.wait( jobs, return_when=concurrent.futures.FIRST_COMPLETED )
            jobs = pending

            for job in finished:
                try:
                    current = job.result( )

                    deps.add( current )

                    for dep_name in current.dependencies:
                        if dep_name not in processed:
                            processed.add( dep_name )

                            new_job = executor.submit( self.get_package, dep_name )
                            job_packages[ new_job ] = dep_name
                            jobs.add( new_job )
                except PackageNotFoundError as e:
                    missing.append( job_packages[ job ] )

        return deps, missing


