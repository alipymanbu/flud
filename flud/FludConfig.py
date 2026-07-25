"""
FludConfig.py, (c) 2003-2006 Alen Peacock.  This program is distributed under
the terms of the GNU General Public License (the GPL), version 3.

manages configuration file for flud backup.
"""

import os, sys, socket, re, logging, time, threading, asyncio, sqlite3
import configparser

import flud.FludCrypto as FludCrypto
from flud.FludCrypto import FludRSA
from flud.FludkRouting import kRouting
from flud.fencode import fencode, fdecode


def _manifest_ancestor_chain(fname):
    """Mirrors FludFileOperations.pathsplit(): the full ancestor chain from
    the filesystem root through fname itself, e.g. "/a/b/c.txt" ->
    ["/", "/a", "/a/b", "/a/b/c.txt"]. Duplicated locally (rather than
    imported) to avoid a circular import with FludFileOperations."""
    par, chld = os.path.split(fname)
    if chld == "":
        return [par]
    return _manifest_ancestor_chain(par) + [os.path.join(par, chld)]

logger = logging.getLogger('flud')


def _parse_loglevel(value):
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("empty log level")
        if text.isdigit():
            return int(text)
        level = getattr(logging, text.upper(), None)
        if isinstance(level, int):
            return level
    raise ValueError("invalid log level %r" % (value,))

CLIENTPORTOFFSET = 500

""" default mapping of trust deltas """
class TrustDeltas:
    INITIAL_SCORE = 1
    POSITIVE_CAP = 500
    NEGATIVE_CAP = -500
    MAX_INC_PERDAY = 100 # XXX: currently unused
    MAX_DEC_PERDAY = -250
    # note: the rest of these are classes so that we can pass them around kind
    # of like enums (identify what was passed by type, instead of by value)
    class PUT_SUCCEED:
        value = 2
    class GET_SUCCEED:
        value = 4
    class VRFY_SUCCEED:
        value = 4
    class FNDN_FAIL:
        value = -1
    class PUT_FAIL:
        value = -2
    class GET_FAIL:
        value = -10
    class VRFY_FAIL:
        value = -10

class FludDebugLogFilter(logging.Filter):
    """
    Keeps all logging levels defined by loggers, but ups level to DEBUG for
    loggers whose namespaces match patterns given by wildcards.
    """
    # XXX: doesn't really interact with all logging levels by all loggers, only
    # with the one defined by the root logger.  If children have stricter
    # loglevels set, this filter won't ever get called on them.

    def __init__(self, wildcardStrings):
        self.setWildcards(wildcardStrings)
        root = logging.getLogger("")
        if hasattr(root, 'fludDebugLogLevel'):
            self.effectiveLevel = root.fludDebugLogLevel
        else:
            self.effectiveLevel = root.getEffectiveLevel()
            self.fludDebugLogLevel = root.getEffectiveLevel()
        root.setLevel(logging.NOTSET)

    def setWildcards(self, wildcardStrings):
        self.wildcards = []
        if not isinstance(wildcardStrings, list):
            wildcardStrings = [wildcardStrings]
        for s in wildcardStrings:
            self.setWildcard(s)

    def setWildcard(self, wildcardString):
        fields = wildcardString.split('.')
        for i, s in enumerate(fields):
            #print "%s:%s" % (i, s)
            if "*" == s:
                fields[i] = r'[\w.]*'
            else:
                try:
                    if s.index(s, '*') > 0:
                        fields[i] = s.replace('*', r'[\w]*')
                except:
                    pass
        regex = "^%s$" % r'\.'.join(fields)
        self.wildcards.append(re.compile(regex))

    def filter(self, record):
        if record.levelno >= self.effectiveLevel:
            return 1
        for w in self.wildcards:
            m = w.match(record.name)
            if m:
                return 1
        return 0

# XXX: refactor out the try/except stuff that could be done with has_key()

class FludConfig:
    """
    Handles configuration for Flud nodes.  Most persistent settings live in
    this class.  
    
    Configuration is kept in the directory specified by FLUDHOME if this value
    is set in the environment, otherwise in HOME/.flud/.  If no existing
    configuration exists, this object will create a configuration with sane
    default values.
    """
    def __init__(self):
        self.Kr = 0
        self.Ku = 0
        self.nodeID = 0
        self.groupIDr = 0
        self.groupIDu = 0
        self.port = -1
        self.reputations = {}
        self.nodes = {}
        self.throttled = {}  # XXX: should persist this to config file
        self.manifest_lock = threading.RLock()  # vestigial: no longer guards
                                                  # anything since the manifest
                                                  # moved to a SQLite-backed
                                                  # tree (see manifest_tree_lock)
        self.manifest_cas = None  # last-published manifest DHT pointer value
                                   # (a {"fmt":2,"root":...} dict once published
                                   # -- see FludFileOperations.UpdateManifest),
                                   # used by the DHT republish loop (FludNode.py)
        self.manifest_tree_lock = None  # lazy asyncio.Lock; see
                                         # _manifest_tree_lock()
        self._manifest_tree_lock_loop = None  # event loop manifest_tree_lock
                                               # is currently bound to
        self._manifest_db = None  # lazy sqlite3 connection; see
                                   # _manifest_db_connect()

        try:
            self.fludhome = os.environ['FLUDHOME']
        except:
            try:
                home = os.environ['HOME']
                self.fludhome = home+"/.flud"
            except:
                logger.warn("cannot determine FLUDHOME.")
                logger.warn("Please set HOME or FLUDHOME environment variable")

        if not os.path.isdir(self.fludhome):
            os.mkdir(self.fludhome, 0o700)

        self.fludconfig = self.fludhome+"/flud.conf"
        self.configParser = configparser.ConfigParser()
        if not os.path.isfile(self.fludconfig):
            conffile = open(self.fludconfig, "w")
        else:
            conffile = open(self.fludconfig, "r")
            self.configParser.read_file(conffile)
        conffile.close()

        logger.info('fludhome = %s' % self.fludhome)
        logger.info('fludconfig = %s' % self.fludconfig)

    def load(self, serverport=None, doLogging=True):
        """
        If serverport is given, it overrides any value that may be in the
        configuration file
        """

        self.logfile, self.loglevel = self._getLoggingConf()
        if doLogging:
            if os.path.isfile(self.logfile):
                os.remove(self.logfile)
            handler = logging.FileHandler(self.logfile)
            formatter = logging.Formatter('%(asctime)s %(filename)s:%(lineno)d'
                    ' %(name)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logging.getLogger("").setLevel(self.loglevel)
            #logger.setLevel(self.loglevel)
            #logger.setLevel(logging.WARNING) # XXX: overrides user prefs
            #logger.setLevel(logging.DEBUG) # XXX: overrides user prefs
            if "LOGFILTER" in os.environ:
                self.filter = FludDebugLogFilter(
                        os.environ["LOGFILTER"].split(' '))
                handler.addFilter(self.filter)
                # XXX: add a LocalPrimitive that can be called dynamically to
                # invoke filter.setWildcards()

        self.Kr, self.Ku, self.nodeID, self.groupIDr, self.groupIDu \
                = self._getID()
        logger.debug('Kr = %s' % self.Kr.exportPrivateKey())
        logger.debug('Ku = %s' % self.Ku.exportPublicKey())
        logger.debug('nodeID = %s' % self.nodeID)
        logger.debug('groupIDr = %s' % self.groupIDr)
        logger.debug('groupIDu = %s' % self.groupIDu)
        
        self.port, self.clientport = self._getServerConf()
        if serverport != None:
            self.port = serverport
            self.clientport = serverport + CLIENTPORTOFFSET
            self._setconf("server", "port", self.port)
            self._setconf("server", "clientport", self.clientport)
        logger.debug('port = %s' % self.port)
        logger.debug('clientport = %s' % self.clientport)
        logger.debug('trustdeltas = %s' 
                % [v for v in dir(TrustDeltas) if v[0] != '_'])

        self.routing = kRouting((socket.getfqdn(), self.port,
                int(self.nodeID, 16), self.Ku.exportPublicKey()['n']))

        self.storedir, self.generosity, self.minoffer = self._getStoreConf()
        if not os.path.isdir(self.storedir):
            os.mkdir(self.storedir)
            os.chmod(self.storedir, 0o700)
        logger.debug('storedir = %s' % self.storedir)

        self.kstoredir = self._getkStoreConf()
        if not os.path.isdir(self.kstoredir):
            os.mkdir(self.kstoredir)
            os.chmod(self.kstoredir, 0o700)
        logger.debug('kstoredir = %s' % self.kstoredir)

        self.clientdir = self._getClientConf()
        if not os.path.isdir(self.clientdir):
            os.mkdir(self.clientdir)
            os.chmod(self.clientdir, 0o700)
        logger.debug('clientdir = %s' % self.clientdir)

        self.metadir, self.manifest_name = self._getMetaConf()
        if not os.path.isdir(self.metadir):
            os.mkdir(self.metadir)
            os.chmod(self.metadir, 0o700)
        logger.debug('metadir = %s' % self.metadir)

        self.reputations = self._getReputations()
        logger.debug("reputations = %s" % str(self.reputations))
        
        self.nodes = self._getKnownNodes()
        logger.debug("known nodes = %s" % str(self.nodes))

        self.save()
        os.chmod(self.fludconfig, 0o600)

        self.loadManifest()

    def save(self):
        """
        saves configuration
        """
        conffile = open(self.fludconfig, "w")
        self.configParser.write(conffile) 
        conffile.close()

    def _setconf(self, section, option, value):
        self.configParser.set(section, option, str(value))

    def _getLoggingConf(self):
        """
        Returns logging configuration: logfile and loglevel 
        """
        if not self.configParser.has_section("logging"):
            self.configParser.add_section("logging")
        
        try:
            logfile = int(self.configParser.get("logging","logfile"))
        except:
            logger.debug("no logfile specified, using default")
            logfile = self.fludhome+'/flud.log'
        self._setconf("logging", "logfile", logfile)

        env_loglevel = os.environ.get("LOGLEVEL")
        if env_loglevel is not None:
            try:
                loglevel = _parse_loglevel(env_loglevel)
            except Exception:
                logger.warning("invalid LOGLEVEL=%r, falling back to config",
                        env_loglevel)
                env_loglevel = None
        if env_loglevel is None:
            try:
                loglevel = _parse_loglevel(
                        self.configParser.get("logging", "loglevel"))
                #loglevel = logging.WARNING # XXX: remove me
            except:
                logger.debug("no loglevel specified, using default")
                loglevel = logging.WARNING
        self._setconf("logging", "loglevel", loglevel)

        return logfile, loglevel 

        
    def _getID(self):
        """
        Returns a tuple: private key, public key, nodeID, private group ID, and
        public group ID from config.  If these values don't exist in conf file,
        they are generated and added.
        """
        # get the keys and IDs from the config file.
        # If these values don't exist, generate a pub/priv key pair, nodeID,
        # and groupIDs.
        if not self.configParser.has_section("identification"):
            self.configParser.add_section("identification")
        
        try:
            privkey = FludRSA.importPrivateKey( 
                    eval(self.configParser.get("identification","Kr"))) 
        except:
            pubkey, privkey = FludCrypto.generateKeys()
        else:
            try:
                pubkey = FludRSA.importPublicKey( 
                        eval(self.configParser.get("identification","Ku")))
            except:
                pubkey = privkey.publickey()
            
        try:
            nodeID = self.configParser.get("identification","nodeID") 
        except:
            #nodeID = FludCrypto.hashstring(str(pubkey.exportPublicKey()))
            nodeID = pubkey.id()
        
        try:
            privgroupID = self.configParser.get("identification",
                    "groupIDr")[:64]
        except:
            privgroupID = 'fludtest' # default groupID hardcoded
        
        try:
            pubgroupID = self.configParser.get("identification","groupIDu") 
        except:
            pubgroupID = FludCrypto.hashstring(str(pubkey.exportPublicKey()) 
                    +privgroupID)

        # write the settings back out to config object
        self._setconf("identification", "Kr", privkey.exportPrivateKey())
        self._setconf("identification", "Ku", pubkey.exportPublicKey())
        self._setconf("identification", "nodeID", nodeID)
        self._setconf("identification", "groupIDr", privgroupID)
        self._setconf("identification", "groupIDu", pubgroupID)
        
        # return the values
        return privkey, pubkey, nodeID, privgroupID, pubgroupID

    def _getServerConf(self):
        """
        Returns server configuration: port number
        """
        if not self.configParser.has_section("server"):
            self.configParser.add_section("server")
        
        try:
            port = int(self.configParser.get("server","port"))
        except:
            logger.debug("no port specified, using default")
            port = 8080 # XXX: default should be defined elsewhere.
                        #      Should prefer 80.  If in use, use 8080+ 
        
        try:
            clientport = int(self.configParser.get("server","clientport"))
        except:
            logger.debug("no clientport specified, using default")
            clientport = port+CLIENTPORTOFFSET 
        
        self._setconf("server", "port", port)
        self._setconf("server", "clientport", clientport)

        return port, clientport

    def _getDirConf(self, configParser, section, default):
        """
        Returns directory configuration
        """
        if not configParser.has_section(section):
            configParser.add_section(section)
        
        try:
            dir = int(self.configParser.get(section,"dir"))
        except:
            logger.debug("no %s directory specified, using default", section)
            dir = self.fludhome+'/'+default

        if not os.path.isdir(dir):
            os.makedirs(dir)

        configParser.set(section, "dir", str(dir))

        return dir 

    def _getClientConf(self):
        """
        Returns client configuration: download directory 
        """
        try:
            trustdeltas = eval(self.configParser.get("client","trustdeltas"))
            for i in trustdeltas:
                if not hasattr(TrustDeltas, i):
                    logger.error("setting non-useful TrustDelta field %s", i)
                setattr(TrustDeltas, i, trustdeltas[i])
        except:
            logger.debug("no trustdeltas specified, using default")

        if not self.configParser.has_section("client"):
            self.configParser.add_section("client")
        self._setconf("client", "trustdeltas",
                dict((v, eval("TrustDeltas.%s" % v)) for v in dir(TrustDeltas)
                    if v[0] != '_'))

        return self._getDirConf(self.configParser, "client", "dl") 

    def _getStoreConf(self):
        """
        Returns data store configuration
        """
        storedir = self._getDirConf(self.configParser, "store", "store")
        try:
            generosity = self.configParser.get("store", "generosity")
        except:
            logger.debug("no generosity specified, using default")
            generosity = 1.5
        try:
            minoffer = self.configParser.get("store", "minoffer")
        except:
            logger.debug("no minoffer specified, using default")
            minoffer = 1024
        return storedir, generosity, minoffer

    def _getkStoreConf(self):
        """
        Returns dht data store configuration
        """
        return self._getDirConf(self.configParser, "kstore", "dht")

    def _getMetaConf(self):
        """
        Returns metadata configuration: metadata directory 
        """
        metadir = self._getDirConf(self.configParser, "metadata", "meta")
        
        try:
            manifest = self.configParser.get("meta","master")
        except:
            logger.debug("no manifest file specified, using default")
            manifest = "manifest"

        if not os.path.isfile(metadir+'/'+manifest):
            f = open(metadir+'/'+manifest, 'w')
            f.close()
        
        return (metadir, manifest)

    def _getReputations(self):
        """
        Returns dict of reputations known to this node
        """
        # XXX: should probably just throw these in with 'nodes' (for efficiency)
        return self._getDict(self.configParser, "reputations")

    def _getKnownNodes(self):
        """
        Returns dict of nodes known to this node
        """
        return {}
        # XXX: don't read known nodes for now
        result = self._getDict(self.configParser, "nodes")
        for i in result:
            print(str(i))
            self.routing.insertNode( 
                    (result[i]['host'], result[i]['port'], int(i, 16), 
                        result[i]['nKu']))
        return result

    def _getDict(self, configParser, section):
        """
        creates a dictionary from the list of pairs given by 
        ConfigParser.items(section).  Requires that the right-hand side of
        the config file's "=" operator be a valid python type, as eval()
        will be invoked on it
        """
        if not configParser.has_section(section):
            configParser.add_section(section)
        
        try:
            items = configParser.items(section)
            result = {}
            for item in items:
                #print item
                try:
                    result[str(item[0])] = eval(item[1])
                    configParser.set(section, item[0], str(item[1]))
                except:
                    logger.warn("item '%s' in section '%s'"
                            " of the config file has an unreadable format" 
                            % str(item[0]), str(section))
        except:
            logger.warn("Couldn't read %s from config file:" % section)

        return result

    def addNode(self, nodeID, host, port, Ku, mygroup=None):
        """
        Convenience method for adding a node to the known.
        If a node with nodeID already exists, nothing changes.
        This method /does not/ save the new configuration to file,
        """
        if mygroup == None:
            mygroup = self.groupIDu
        if nodeID not in self.nodes:
            self.nodes[nodeID] = {'host': host, 'port': port, 
                    'Ku': Ku.exportPublicKey(), 'mygroup': mygroup}
            #logger.log(logging.DEBUG, "nodes: " % str(self.nodes))
            # XXX: disabled nodes saving
            #for k in self.nodes:
            #   self._setconf('nodes', k, self.nodes[k])
            n = self.routing.insertNode((host, int(port), int(nodeID, 16), 
                Ku.exportPublicKey()['n']))
            if n != None:
                logger.warn("need to ping %s for LRU in routing table!" 
                        % str(n))
                # XXX: instead of pinging, put it in a replacement cache table
                #      and when one of the nodes needs replaced (future query)
                #      replace it with one of these. Sec 4.1
                self.routing.replacementCache.insertNode(
                        (host, int(port), int(nodeID, 16),
                            Ku.exportPublicKey()['n']))
            self.reputations[int(nodeID,16)] = TrustDeltas.INITIAL_SCORE
            # XXX: no management of reputations size: need to manage as a cache
    
    def modifyReputation(self, nodeID, reason):
        """
        change reputation of nodeID by reason.value
        """
        logger.info("modify %s %s" % (nodeID, reason.value))
        if isinstance(nodeID, str):
            nodeID = int(nodeID,16)
        if nodeID not in self.reputations:
            self.reputations[nodeID] = TrustDeltas.INITIAL_SCORE
            # XXX: no management of reputations size: need to manage as a cache
        self.reputations[nodeID] += reason.value
        logger.debug("reputation for %d now %d", nodeID, 
                self.reputations[nodeID])
        curtime = int(time.time())
        if reason.value < 0:
            self.throttleNode(nodeID, reason, curtime)
        elif nodeID in self.throttled and self.throttled[nodeID] < curtime:
            self.throttled.pop(nodeID)

    def throttleNode(self, nodeID, reason, curtime=None):
        """
        puts a node in the throttle list.
        """
        if not curtime:
            curtime = int(time.time())
        pause = curtime \
                + (reason.value * 24 * 60 * 60) / TrustDeltas.MAX_DEC_PERDAY
        self.throttled[nodeID] = pause 

    def getPreferredNodes(self, num=None, exclude=None, throttle=False):
        """
        Get nodes ordered by reputation.  If num is passed in, return the first
        'num' nodes, otherwise all.  If exclude list is passed in, try to
        return nodes not on this list (but do return some excluded if nodes are
        exhausted, i.e., there aren't num nodes available).  If throttle
        (default), do not return any nodes which are currently throttled.
        """
        # XXX: O(n) each time this is called.  Better performance if we
        # maintain sorted list when modified (modifyReputation, addNode), at a
        # bit higher mem expense.
        items = list(self.reputations.items())
        numitems = len(items)
        logger.debug("%d items in reps" % numitems)
        if throttle:

            now = int(time.time())
            for t in self.throttled:
                if self.throttled[t] < now:
                    self.throttled.pop(t)

            if exclude:
                items = [(v,k) for (k,v) in items if k not in self.throttled and 
                        k not in exclude]
                if num and len(items) < num and numitems >= num:
                    exitems = [(v,k) for (k,v) in self.reputations.items()
                            if k not in self.throttled and k in exclude]
                    items += exitems[:num-len(items)]
                logger.debug("%d items now in reps" % len(items))
            else:
                items = [(v,k) for (k,v) in items if k not in self.throttled]
        
        else:
            # XXX: refactor; 'if exclude else' is same as above, but without
            # the 'if k not in throttle' bits
            if exclude:
                items = [(v,k) for (k,v) in items if k not in exclude]
                if num and len(items) < num and numitems >= num:
                    exitems = [(v,k) for (k,v) in self.reputations.items()
                            if k in exclude]
                    items += exitems[:num-len(items)]
                logger.debug("%d items now in reps" % len(items))
            else:
                items = [(v,k) for (k,v) in items]

        items.sort()
        items.reverse()
        items = [(k,v) for (v,k) in items]
        # need to call routing.getNode() to get node triple and return those
        if num:
            logger.debug("returning %d of the %d items" % (num, len(items)))
            return [self.routing.getNode(f) for (f,v) in items[:num]]
        else:
            logger.debug("returning all %d of the items" % len(items))
            return [self.routing.getNode(f) for (f,v) in items]

    # The manifest is a content-addressed tree, mirroring the local
    # filesystem hierarchy: each directory is its own small object listing
    # its children (a bare (sK, timestamp) tuple for a file child, or a
    # {"hash": ...} reference for a subdirectory child); only the root's
    # hash is a mutable pointer. Locally cached in a small SQLite db
    # (metadir/manifest.db); the same node objects are individually
    # k_store'd to the DHT (see FludFileOperations.UpdateManifest). See
    # flud/docs/dht-metadata-performance.md tier B for the design rationale.
    #
    # updateManifest/getFromManifest/deleteFromManifest keep their original
    # (path, value) / (path) -> value call signatures so most callers in
    # FludFileOperations.py don't need to change, but are now async and
    # operate on the tree instead of a flat dict.

    def _manifest_tree_lock(self):
        # asyncio.Lock binds to whichever event loop is running the first
        # time it's acquired, and raises if later acquired from a
        # different loop. FludConfig objects can legitimately outlive a
        # single event loop (e.g. a long-lived shared node reused across
        # several separate asyncio.run() calls, as flud's own test suite
        # does with session/module-scoped node fixtures) -- recreate the
        # lock whenever the running loop has changed, rather than reusing
        # one bound to a now-closed loop. Safe: asyncio guarantees only one
        # loop runs at a time per thread, so a loop change means the old
        # loop (and anything that might have been contending for the old
        # lock) is no longer running.
        loop = asyncio.get_running_loop()
        if self._manifest_tree_lock_loop is not loop:
            self.manifest_tree_lock = asyncio.Lock()
            self._manifest_tree_lock_loop = loop
        return self.manifest_tree_lock

    def _manifest_db_connect(self):
        if self._manifest_db is None:
            db_path = os.path.join(self.metadir, "manifest.db")
            db = sqlite3.connect(db_path, check_same_thread=False)
            # WAL mode lets unprotected reads (republish/PUTM tree walks,
            # which deliberately run outside manifest_tree_lock per the
            # design doc) proceed concurrently with a lock-protected write,
            # without blocking on or corrupting either.
            db.execute("PRAGMA journal_mode=WAL")
            db.execute(
                "CREATE TABLE IF NOT EXISTS nodes "
                "(hash TEXT PRIMARY KEY, blob BLOB NOT NULL)")
            db.execute(
                "CREATE TABLE IF NOT EXISTS root "
                "(id INTEGER PRIMARY KEY CHECK (id = 0), root_hash TEXT)")
            db.commit()
            self._manifest_db = db
        return self._manifest_db

    @staticmethod
    def _manifest_canonical_children(children):
        return sorted(children.items(), key=lambda kv: kv[0])

    def _manifest_node_hash(self, node):
        canonical = {
            "meta": node.get("meta"),
            "children": self._manifest_canonical_children(
                node.get("children", {})),
        }
        return FludCrypto.hashstring(fencode(canonical))

    def _manifest_get_node_sync(self, node_hash):
        if node_hash is None:
            return None
        db = self._manifest_db_connect()
        row = db.execute(
            "SELECT blob FROM nodes WHERE hash = ?", (node_hash,)).fetchone()
        if row is None:
            return None
        return fdecode(row[0])

    def _manifest_put_node_sync(self, node):
        node_hash = self._manifest_node_hash(node)
        db = self._manifest_db_connect()
        blob = fencode(node)
        if isinstance(blob, str):
            blob = blob.encode("utf-8")
        db.execute(
            "INSERT OR REPLACE INTO nodes (hash, blob) VALUES (?, ?)",
            (node_hash, blob))
        db.commit()
        return node_hash

    def _manifest_get_root_sync(self):
        db = self._manifest_db_connect()
        row = db.execute("SELECT root_hash FROM root WHERE id = 0").fetchone()
        if row is None or row[0] is None:
            return None
        return row[0]

    def _manifest_set_root_sync(self, node_hash):
        db = self._manifest_db_connect()
        db.execute(
            "INSERT INTO root (id, root_hash) VALUES (0, ?) "
            "ON CONFLICT(id) DO UPDATE SET root_hash = excluded.root_hash",
            (node_hash,))
        db.commit()

    def _manifest_apply_update_sync(self, fname, val):
        """Sets fname's value in the tree (a bare (sK, timestamp) tuple for
        a file, or a filemetadata()-shaped dict for a directory) and
        propagates the resulting hash change up through every ancestor to
        the root. Local-only (never touches the network) -- must be called
        with the manifest tree lock held (or during single-threaded
        startup migration, before any concurrency is possible)."""
        chain = _manifest_ancestor_chain(fname)

        if len(chain) == 1:
            # fname IS the tree root itself -- a root meta update.
            root_hash = self._manifest_get_root_sync()
            node = self._manifest_get_node_sync(root_hash) \
                or {"meta": None, "children": {}}
            node["meta"] = val
            self._manifest_set_root_sync(self._manifest_put_node_sync(node))
            return

        ancestor_paths = chain[:-1]  # root .. fname's immediate parent
        target_name = os.path.basename(fname)

        # Resolve each ancestor's current node top-down (local-only;
        # missing nodes are treated as freshly-created empty ones).
        hashes = [self._manifest_get_root_sync()]
        nodes = []
        for idx, path in enumerate(ancestor_paths):
            node = self._manifest_get_node_sync(hashes[idx]) \
                or {"meta": None, "children": {}}
            nodes.append(node)
            if idx + 1 < len(ancestor_paths):
                child_name = os.path.basename(ancestor_paths[idx + 1])
                ref = node["children"].get(child_name)
                hashes.append(
                    ref["hash"] if isinstance(ref, dict) and "hash" in ref
                    else None)

        deepest = nodes[-1]
        if isinstance(val, dict):
            # fname is itself a directory: fetch/create its own node and
            # set its meta, preserving any children already recorded under
            # it (e.g. a file stored moments earlier in the same
            # StoreFile call, before this ancestor's meta was assigned).
            existing_ref = deepest["children"].get(target_name)
            existing_hash = (
                existing_ref["hash"]
                if isinstance(existing_ref, dict) and "hash" in existing_ref
                else None)
            child_node = self._manifest_get_node_sync(existing_hash) \
                or {"meta": None, "children": {}}
            child_node["meta"] = val
            child_hash = self._manifest_put_node_sync(child_node)
            deepest["children"][target_name] = {"hash": child_hash}
        else:
            # fname is a file: embed its (sK, timestamp) ref directly --
            # no separate node/hash-hop needed for leaves.
            deepest["children"][target_name] = val

        new_hash = self._manifest_put_node_sync(deepest)
        for idx in range(len(nodes) - 2, -1, -1):
            parent_node = nodes[idx]
            child_name = os.path.basename(ancestor_paths[idx + 1])
            parent_node["children"][child_name] = {"hash": new_hash}
            new_hash = self._manifest_put_node_sync(parent_node)
        self._manifest_set_root_sync(new_hash)

    def _manifest_resolve_node_sync(self, path):
        """Local-only path resolution: walks from root to `path`, one
        component at a time. Returns the dirnode dict at `path`, the bare
        (sK, timestamp) tuple if `path` names a file, or None if not
        found locally."""
        chain = _manifest_ancestor_chain(path)
        node = self._manifest_get_node_sync(self._manifest_get_root_sync())
        if node is None:
            return None
        if len(chain) == 1:
            return node
        for component_path in chain[1:]:
            name = os.path.basename(component_path)
            ref = node["children"].get(name)
            if ref is None:
                return None
            if isinstance(ref, dict) and "hash" in ref:
                node = self._manifest_get_node_sync(ref["hash"])
                if node is None:
                    return None
            elif component_path == chain[-1]:
                return ref  # file leaf
            else:
                return None  # file ref encountered mid-path -- invalid
        return node

    async def updateManifest(self, fname, val):
        """
        update fname with val: a bare (sK, timestamp) tuple for a file, or
        a filemetadata()-shaped dict for a directory.
        """
        async with self._manifest_tree_lock():
            await asyncio.to_thread(self._manifest_apply_update_sync, fname, val)

    async def getFromManifest(self, fname):
        """
        get val for fname: a (sK, timestamp) tuple for a file, a
        filemetadata()-shaped dict (or None if not yet set) for a
        directory, or None if fname isn't present at all.
        """
        async with self._manifest_tree_lock():
            result = await asyncio.to_thread(
                self._manifest_resolve_node_sync, fname)
        if result is None:
            return None
        if isinstance(result, dict):
            return result.get("meta")
        return result

    async def listManifestChildren(self, path):
        """
        Returns the immediate children of the directory at `path` (a dict
        of {name: (sK, timestamp) | {"hash": ...}}), or None if `path`
        doesn't resolve to a directory. Local-only.
        """
        async with self._manifest_tree_lock():
            node = await asyncio.to_thread(
                self._manifest_resolve_node_sync, path)
        if not isinstance(node, dict):
            return None
        return dict(node.get("children", {}))

    async def deleteFromManifest(self, fname):
        """
        Deletion isn't implemented: it would require the same bottom-up
        rehash-and-propagate machinery as updateManifest, plus a decision
        about pruning now-empty ancestor dirnodes, and has zero callers
        anywhere in the codebase today. Logs and no-ops rather than
        silently doing nothing incorrect or crashing.
        """
        logger.warning(
            "deleteFromManifest(%s) called but is not implemented "
            "(tree-based manifest, no callers exist yet)", fname)

    def _manifest_walk_reachable_sync(self, root_hash):
        """Returns [(hash, node_dict), ...] for every node reachable from
        root_hash (BFS over subdirectory {"hash": ...} refs). Read-only,
        local-only. Used to publish/republish the tree to the DHT."""
        if root_hash is None:
            return []
        seen = {}
        queue = [root_hash]
        while queue:
            h = queue.pop()
            if h in seen:
                continue
            node = self._manifest_get_node_sync(h)
            if node is None:
                continue
            seen[h] = node
            for ref in node.get("children", {}).values():
                if isinstance(ref, dict) and "hash" in ref:
                    queue.append(ref["hash"])
        return list(seen.items())

    async def snapshotManifestRoot(self):
        """Briefly locks to read the current root hash consistently, then
        releases -- callers should walk/publish outside the lock (see
        walkManifestReachable) so a long walk doesn't block concurrent
        local writes."""
        async with self._manifest_tree_lock():
            return self._manifest_get_root_sync()

    async def walkManifestReachable(self, root_hash):
        """[(hash, node_dict), ...] for every node reachable from
        root_hash. Local-only, read-only, deliberately NOT lock-protected
        (see snapshotManifestRoot) -- WAL mode allows this to run
        concurrently with in-flight local writes without blocking either."""
        return await asyncio.to_thread(
            self._manifest_walk_reachable_sync, root_hash)

    async def getManifestNode(self, node_hash):
        """Fetches a single node by hash, local-only (None if not cached
        locally). Used for disaster-recovery-style reconstruction, where
        each network-fetched node is inserted via putManifestNode."""
        return await asyncio.to_thread(self._manifest_get_node_sync, node_hash)

    async def putManifestNode(self, node):
        """Inserts a node (already fetched from the network, or freshly
        built) into local storage, returning its hash. Does NOT touch the
        root pointer -- callers set that separately via setManifestRoot
        once the whole tree they care about is locally present."""
        return await asyncio.to_thread(self._manifest_put_node_sync, node)

    async def setManifestRoot(self, root_hash):
        await asyncio.to_thread(self._manifest_set_root_sync, root_hash)

    def loadManifest(self):
        """
        Opens (creating if needed) the local manifest tree db. If this
        node has an old-format flat-file manifest but no tree yet,
        migrates it once: the old file's entries are replayed into the
        tree purely from local data (no network needed), and the old file
        is preserved (renamed), not deleted, so a rollback to old code
        degrades gracefully instead of crashing on a missing file.
        """
        self._manifest_db_connect()
        if self._manifest_get_root_sync() is not None:
            return  # already using the tree format

        manifest_path = os.path.join(self.metadir, self.manifest_name)
        if not os.path.isfile(manifest_path):
            return  # brand new node, nothing to migrate
        with open(manifest_path, 'r') as f:
            raw = f.read()
        if raw == "":
            return
        old_manifest = fdecode(raw)
        if not old_manifest:
            return

        logger.info(
            "migrating manifest from flat-dict format to content-addressed "
            "tree (%d entries)", len(old_manifest))
        for old_fname, old_val in old_manifest.items():
            self._manifest_apply_update_sync(old_fname, old_val)

        backup_path = manifest_path + ".pre-tree-migration"
        os.rename(manifest_path, backup_path)
        logger.info(
            "manifest migration complete; old manifest preserved at %s",
            backup_path)

    def syncManifest(self):
        """
        Vestigial: every tree mutation now commits directly to
        manifest.db as it happens, so there's no in-memory state left to
        flush. Kept as a no-op so existing callers don't need to change.
        """
        pass
        
    def _test(self):
        import doctest
        doctest.testmod()

if __name__ == '__main__':
    fludConfig = FludConfig()
    fludConfig._test()
