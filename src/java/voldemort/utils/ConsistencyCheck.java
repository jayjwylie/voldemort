package voldemort.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ConsistencyCheck {

    private final Boolean verbose;
    private final List<String> urls;
    private final String storeName;
    private final Integer partitionId;

    private Integer retentionDays = 0;
    private Integer replicationFactor = 0;
    private Integer requiredWrites = 0;
    private Map<PrefixNode, Iterator<Pair<ByteArray, Versioned<byte[]>>>> nodeEntriesMap;

    public ConsistencyCheck(List<String> urls, String storeName, int partitionId, boolean verbose) {
        this.urls = urls;
        this.storeName = storeName;
        this.partitionId = partitionId;
        this.verbose = verbose;

    }

    /**
     * Connect to the clusters using given urls and start fetching process on
     * correct nodes
     * 
     * @throws Exception When no such store is found
     */
    public void connect() throws Exception {
        List<Integer> singlePartition = new ArrayList<Integer>();
        singlePartition.add(partitionId);

        nodeEntriesMap = new HashMap<PrefixNode, Iterator<Pair<ByteArray, Versioned<byte[]>>>>();

        List<AdminClient> adminClients = new ArrayList<AdminClient>(urls.size());
        int urlId = 0;
        List<PrefixNode> nodeList = new ArrayList<PrefixNode>();

        for(String url: this.urls) {
            /* connect to cluster through admin port */
            if(this.verbose) {
                System.out.println("Connecting to bootstrap server: " + url);
            }
            // TODO: adminClient.stop() is never explicitly called. I think it
            // should be.
            AdminClient adminClient = new AdminClient(url, new AdminClientConfig(), 0);
            adminClients.add(adminClient);
            Cluster cluster = adminClient.getAdminClientCluster();

            /* find store */
            Versioned<List<StoreDefinition>> storeDefinitions = adminClient.metadataMgmtOps.getRemoteStoreDefList(0);
            // TODO: (refactor) once you've merged, I think there is a helper
            // function to get StoreDef by name.
            StoreDefinition storeDefinition = null;
            for(StoreDefinition def: storeDefinitions.getValue()) {
                if(def.getName().equals(storeName)) {
                    storeDefinition = def;
                    break;
                }
            }
            if(storeDefinition == null) {
                throw new Exception("No such store found: " + storeName);
            }

            // TODO: Does retentionDays need to be calculated for each url? I
            // think the 'retentionDays==0' check is to make sure it is only
            // determined once. This seems a bit awkward. Not sure how to clean
            // up. But, maybe retentionDays should be read from each cluster's
            // store def to confirm it is the same everywhere?
            /* find the store retention policy */
            int storeRetentionDays = 0;
            if(storeDefinition.getRetentionDays() != null) {
                storeRetentionDays = storeDefinition.getRetentionDays().intValue();
            }
            if((retentionDays == 0)
               || (storeRetentionDays != 0 && storeRetentionDays < retentionDays)) {
                retentionDays = storeRetentionDays;
            }

            /* make partitionId -> node mapping */
            // TODO: (refactor) I think there is a helper method in the merged
            // code that dose the following. If not, there should be. And, the
            // Cluster would have excetped at construction if there were
            // duplicate partition Ids, so that check is unnecessary.
            SortedMap<Integer, Node> partitionToNodeMap = new TreeMap<Integer, Node>();
            Collection<Node> nodes = cluster.getNodes();
            for(Node n: nodes) {
                for(Integer partition: n.getPartitionIds()) {
                    if(partitionToNodeMap.containsKey(partition))
                        throw new IllegalArgumentException("Duplicate partition id " + partition
                                                           + " in cluster configuration " + nodes);
                    partitionToNodeMap.put(partition, n);
                }
            }

            /* find list of nodeId hosting partition */
            // TODO: (refactor) Again, I think there is something that mostly
            // does the following. Use (or add) common method and then convert
            // the list of nodes to a list of PrefixNodes.
            List<Integer> partitionList = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition,
                                                                                             cluster)
                                                                      .getReplicatingPartitionList(partitionId);
            List<Integer> nodeIdList = new ArrayList<Integer>(partitionList.size());
            for(int partition: partitionList) {
                Integer nodeId = partitionToNodeMap.get(partition).getId();
                nodeIdList.add(nodeId);
                nodeList.add(new PrefixNode(urlId, cluster.getNodeById(nodeId)));
            }

            /* print config info */
            if(verbose) {
                StringBuilder configInfo = new StringBuilder();
                configInfo.append("TYPE,Store,PartitionId,Node,ZoneId,BootstrapUrl\n");
                for(Integer nodeId: nodeIdList) {
                    configInfo.append("CONFIG,");
                    configInfo.append(storeName + ",");
                    configInfo.append(partitionId + ",");
                    configInfo.append(nodeId + ",");
                    configInfo.append(cluster.getNodeById(nodeId).getZoneId() + ",");
                    configInfo.append(url + "\n");
                }
                System.out.println(configInfo);
            }

            /* get entry Iterator from each node */
            for(Integer nodeId: nodeIdList) {
                Iterator<Pair<ByteArray, Versioned<byte[]>>> entries;
                entries = adminClient.bulkFetchOps.fetchEntries(nodeId,
                                                                storeName,
                                                                singlePartition,
                                                                null,
                                                                false);
                nodeEntriesMap.put(new PrefixNode(urlId, cluster.getNodeById(nodeId)), entries);
            }

            // TODO: replicationFactor and requiredWrites are also a per
            // cluster/store parameter (like retentionDays). Maybe it would be
            // cleaner to capture retentionDays, replicationFactor, and
            // requiredWrites for each store in some map<url,struct> and then
            // post-process the struct afte the loop to check for invariants and
            // determine aggregations (such as replicationFactor)?

            // calculate overall replication factor and required writes
            replicationFactor += storeDefinition.getReplicationFactor();
            if(requiredWrites == 0) {
                requiredWrites = storeDefinition.getRequiredWrites();
            }
            urlId++;
        }
    }

    // TODO: This method is long. Anyway to break out some sub-parts?
    /**
     * Run consistency check on connected key-value iterators
     * 
     * @return Results in form of ConsistencyCheckStats
     */
    public ConsistencyCheckStats execute() {
        // retention checker
        RetentionChecker retentionChecker = new RetentionChecker(retentionDays);

        // map to remember key-version-node information
        Map<ByteArray, Map<Version, Set<PrefixNode>>> keyVersionNodeSetMap;
        keyVersionNodeSetMap = new HashMap<ByteArray, Map<Version, Set<PrefixNode>>>();

        // variables to sweep good keys on the fly
        // TODO: any way to pull these two members into a class with a couple
        // methods on them? I think that would break the complicated accounting
        // logic below out from the logic of fetching and determining
        // consistency level.
        Map<ByteArray, Set<Iterator<Pair<ByteArray, Versioned<byte[]>>>>> fullyFetchedKeys;
        fullyFetchedKeys = new HashMap<ByteArray, Set<Iterator<Pair<ByteArray, Versioned<byte[]>>>>>();
        Map<Iterator<Pair<ByteArray, Versioned<byte[]>>>, ByteArray> lastFetchedKey;
        lastFetchedKey = new HashMap<Iterator<Pair<ByteArray, Versioned<byte[]>>>, ByteArray>();

        /* start fetches */
        boolean anyNodeHasNext;
        long consistentKeys = 0;
        ProgressReporter reporter = new ProgressReporter();
        do {
            anyNodeHasNext = false;
            /* for each iterator(fetch one key at a time) */
            // TODO: (commentary) If I understand this logic correctly, then all
            // servers are iterated over at the same rate, regardless of how far
            // behind/ahead they are from one another. I think there is probably
            // a simple test before nodeEntreis.hasNext() to determine if the
            // node in question is ahead of all the others; if so, then
            // 'continue'. Such a check would protect against ugly corner-cases
            // in which some server is missing a ton of keys and so has giant
            // gaps in its keyspace. I don't think this is worht worrying about,
            // but I am noting this corner-case in case you think it ought to be
            // documented or protected against.
            for(Map.Entry<PrefixNode, Iterator<Pair<ByteArray, Versioned<byte[]>>>> nodeEntriesMapEntry: nodeEntriesMap.entrySet()) {
                PrefixNode node = nodeEntriesMapEntry.getKey();
                Iterator<Pair<ByteArray, Versioned<byte[]>>> nodeEntries = nodeEntriesMapEntry.getValue();
                if(nodeEntries.hasNext()) {
                    anyNodeHasNext = true;
                    reporter.recordScans(1);
                    Pair<ByteArray, Versioned<byte[]>> nodeEntry = nodeEntries.next();
                    ByteArray key = nodeEntry.getFirst();
                    Versioned<byte[]> versioned = nodeEntry.getSecond();
                    Version version;
                    if(urls.size() == 1) {
                        version = nodeEntry.getSecond().getVersion();
                    } else {
                        version = new HashedValue(versioned);
                    }
                    // skip version if expired
                    if(retentionChecker.isExpired(version)) {
                        reporter.recordExpired(1);
                        continue;
                    }
                    // try sweep last key fetched by this iterator
                    if(lastFetchedKey.containsKey(nodeEntries)) {
                        ByteArray lastKey = lastFetchedKey.get(nodeEntries);
                        if(!key.equals(lastKey)) {
                            if(!fullyFetchedKeys.containsKey(lastKey)) {
                                fullyFetchedKeys.put(lastKey,
                                                     new HashSet<Iterator<Pair<ByteArray, Versioned<byte[]>>>>());
                            }
                            Set<Iterator<Pair<ByteArray, Versioned<byte[]>>>> lastKeyIterSet = fullyFetchedKeys.get(lastKey);
                            lastKeyIterSet.add(nodeEntries);

                            // sweep if fully fetched by all iterators
                            if(lastKeyIterSet.size() == nodeEntriesMap.size()) {
                                // keyFetchComplete
                                ConsistencyLevel level = determineConsistency(keyVersionNodeSetMap.get(lastKey),
                                                                              replicationFactor);
                                if(level == ConsistencyLevel.FULL
                                   || level == ConsistencyLevel.LATEST_CONSISTENT) {
                                    keyVersionNodeSetMap.remove(lastKey);
                                    consistentKeys++;
                                }
                                fullyFetchedKeys.remove(lastKey);
                            }
                        }
                    }
                    // remember key fetch states
                    lastFetchedKey.put(nodeEntries, key);
                    // initialize key -> Map<Version, Set<nodeId>>
                    if(!keyVersionNodeSetMap.containsKey(key)) {
                        keyVersionNodeSetMap.put(key, new HashMap<Version, Set<PrefixNode>>());
                    }
                    Map<Version, Set<PrefixNode>> versionNodeSetMap = keyVersionNodeSetMap.get(key);
                    // Initialize Version -> Set<nodeId>
                    if(!versionNodeSetMap.containsKey(version)) {
                        // decide if this is the newest version
                        Iterator<Version> iter = versionNodeSetMap.keySet().iterator();
                        // if after any one in the map, then reset map
                        if(iter.hasNext()) {
                            Version existingVersion = iter.next();
                            // existing version(s) are old
                            if(version.compare(existingVersion) == Occurred.AFTER) {
                                // swap out the old map and put a new
                                // map
                                versionNodeSetMap = new HashMap<Version, Set<PrefixNode>>();
                                keyVersionNodeSetMap.put(key, versionNodeSetMap);
                            } else if(existingVersion.compare(version) == Occurred.AFTER) {
                                // ignore this version
                                continue;
                            } else if(existingVersion.compare(version) == Occurred.CONCURRENTLY) {
                                // put it into the node set
                            } else {
                                // TODO: sufficient to just dump to System.err?
                                // Does this warrant aborting? This feels like
                                // an unreachable code path and so I'd prefer
                                // abort...
                                System.err.print("[ERROR]Two versions are not after each other nor currently(key, v1, v2)");
                                System.err.print(key + ", " + existingVersion + ", " + version);
                            }
                        }
                        // insert nodeIdSet into the map
                        versionNodeSetMap.put(version, new HashSet<PrefixNode>());
                    }
                    // add nodeId to set
                    Set<PrefixNode> nodeSet = versionNodeSetMap.get(version);
                    nodeSet.add(node);
                }
            }
            // stats reporting
            if(verbose) {
                reporter.tryReport();
            }
        } while(anyNodeHasNext);

        // clean keys not sufficient for write
        cleanIneligibleKeys(keyVersionNodeSetMap, requiredWrites);

        // clean the rest of consistent keys
        Set<ByteArray> keysToDelete = new HashSet<ByteArray>();
        for(ByteArray key: keyVersionNodeSetMap.keySet()) {
            ConsistencyLevel level = determineConsistency(keyVersionNodeSetMap.get(key),
                                                          replicationFactor);
            if(level == ConsistencyLevel.FULL || level == ConsistencyLevel.LATEST_CONSISTENT) {
                keysToDelete.add(key);
            }
        }
        for(ByteArray key: keysToDelete) {
            keyVersionNodeSetMap.remove(key);
            consistentKeys++;
        }

        // print inconsistent keys
        if(verbose) {
            // TODO: I don't think this should be wrapped in 'if(verbose)'. This
            // is the rationale for running the checker and not printing these
            // out would be pointless (I think). Could also consider specifying
            // an output file for the bad keys to keep this important output
            // distinct from all the other verbose output.
            // TODO: (refactor) Should logger be used instead of system.out?
            System.out.println("TYPE,Store,ParId,Key,ServerSet,VersionTS,VectorClock[,ValueHash]");
            for(Map.Entry<ByteArray, Map<Version, Set<PrefixNode>>> entry: keyVersionNodeSetMap.entrySet()) {
                ByteArray key = entry.getKey();
                Map<Version, Set<PrefixNode>> versionMap = entry.getValue();
                System.out.print(keyVersionToString(key, versionMap, storeName, partitionId));
            }
        }

        ConsistencyCheckStats stats = new ConsistencyCheckStats();
        stats.consistentKeys = consistentKeys;
        stats.totalKeys = keyVersionNodeSetMap.size() + consistentKeys;

        return stats;
    }

    // TODO: (refactor) though not needed, a ConsistencyLevel of 'Ineligible',
    // 'ignorable', or 'incomplete' may help with code clarity. You currently
    // have comments and method names that use various words. Having a single
    // specific word/enum will help with logic/code clarity.
    protected enum ConsistencyLevel {
        FULL,
        LATEST_CONSISTENT,
        INCONSISTENT
    }

    // TODO: (refactor) I'd call this ClusterNode and I would have used the
    // cluster's url or its name rather than maintain a counter to generate
    // unique integer values for each such url/name. At least rename the method,
    // but maybe keep the integer prefixId. Also, should the javadoc comment
    // precede the class or the constructor? Your comment on the constructor
    // seems more like the comment for the class.
    protected static class PrefixNode {

        private Integer prefixId;
        private Node node;

        /**
         * Used to track nodes that may share the same nodeId in different
         * clusters
         * 
         * @param prefixId a prefix to be associated different clusters
         * @param node the real node
         */
        public PrefixNode(Integer prefixId, Node node) {
            this.prefixId = prefixId;
            this.node = node;
        }

        public Node getNode() {
            return node;
        }

        public Integer getPrefixId() {
            return prefixId;
        }

        @Override
        public boolean equals(Object o) {
            if(this == o)
                return true;
            if(!(o instanceof PrefixNode))
                return false;

            PrefixNode n = (PrefixNode) o;
            return prefixId.equals(n.getPrefixId()) && node.equals(n.getNode());
        }

        @Override
        public String toString() {
            return prefixId + "." + node.getId();
        }

    }

    protected static class ConsistencyCheckStats {

        public long consistentKeys;
        public long totalKeys;

        /**
         * Used to track consistency results
         */
        public ConsistencyCheckStats() {
            consistentKeys = 0;
            totalKeys = 0;
        }

        public void append(ConsistencyCheckStats that) {
            consistentKeys += that.consistentKeys;
            totalKeys += that.totalKeys;
        }
    }

    // TODO: (refactor) javadoc description of the constructor describes the
    // class.
    protected static class HashedValue implements Version {

        final private Version innerVersion;
        final private Integer valueHash;

        /**
         * A class to save version and value hash It is used to compare versions
         * by the value hash
         * 
         * @param versioned Versioned value with version information and value
         *        itself
         */
        public HashedValue(Versioned<byte[]> versioned) {
            innerVersion = versioned.getVersion();
            valueHash = new FnvHashFunction().hash(versioned.getValue());
        }

        public int getValueHash() {
            return valueHash;
        }

        public Version getInner() {
            return innerVersion;
        }

        @Override
        public boolean equals(Object object) {
            if(this == object)
                return true;
            if(object == null)
                return false;
            if(!object.getClass().equals(HashedValue.class))
                return false;
            HashedValue hash = (HashedValue) object;
            boolean result = valueHash.equals(hash.getValueHash());
            return result;
        }

        @Override
        public int hashCode() {
            return valueHash;
        }

        @Override
        public Occurred compare(Version v) {
            return Occurred.CONCURRENTLY; // always regard as conflict
        }
    }

    protected static class RetentionChecker {

        final private long bufferTimeSeconds = 600; // expire N seconds earlier
        final private long expiredTimeMs;

        /**
         * A checker to determine if a key is to be cleaned according to
         * retention policy
         * 
         * @param days number of days ago from now to retain keys
         */
        public RetentionChecker(int days) {
            if(days <= 0) {
                expiredTimeMs = 0;
            } else {
                long now = System.currentTimeMillis();
                // TODO: (refactor) I prefer breaking conversions of different
                // units onto separate lines. I also think we should not write
                // any new code that uses "voldemort.Time" when TimeUnit exists
                // and is standard java.
                /*-
                long expirationTimeS = TimeUnit.DAYS.toSeconds(days) - bufferTimeSeconds;
                expiredTimeMs = now - TimeUnit.SECONDS.toMillis(expirationTimeS);
                 */
                expiredTimeMs = now - (Time.SECONDS_PER_DAY * days - bufferTimeSeconds)
                                * Time.MS_PER_SECOND;
            }
        }

        /**
         * Determine if a version is expired
         * 
         * @param v version to be checked
         * @return if the version is expired according to retention policy
         */
        public boolean isExpired(Version v) {
            if(v instanceof VectorClock) {
                return ((VectorClock) v).getTimestamp() < expiredTimeMs;
            } else if(v instanceof HashedValue) {
                return false;
            } else {
                System.err.println("[WARNING]Version type is not supported for checking expiration");
                return false;
            }
        }
    }

    protected static class ProgressReporter {

        long lastReportTimeMs = 0;
        long reportPeriodMs = 0;
        long numRecordsScanned = 0;
        long numRecordsScannedLast = 0;
        long numExpiredRecords = 0;

        public ProgressReporter() {
            reportPeriodMs = 5000;
        }

        /**
         * Progress Reporter
         * 
         * @param intervalMs interval between printing progress in miliseconds
         */
        public ProgressReporter(long intervalMs) {
            reportPeriodMs = intervalMs;
        }

        public void recordScans(long count) {
            numRecordsScanned += count;
        }

        public void recordExpired(long count) {
            numExpiredRecords += count;
        }

        public void tryReport() {
            if(System.currentTimeMillis() > lastReportTimeMs + reportPeriodMs) {
                long currentTimeMs = System.currentTimeMillis();
                StringBuilder s = new StringBuilder();
                s.append("Progress Report\n");
                s.append("===============\n");
                s.append("    Number of records Scanned: " + numRecordsScanned + "\n");
                s.append("   Records Ignored(Retention): " + numExpiredRecords + "\n");
                s.append("Recent fetch speed(records/s): "
                         + (numRecordsScanned - numRecordsScannedLast)
                         / ((currentTimeMs - lastReportTimeMs) / 1000) + "\n");
                System.out.print(s.toString());
                lastReportTimeMs = currentTimeMs;
                numRecordsScannedLast = numRecordsScanned;
            }
        }
    }

    /**
     * Return args parser
     * 
     * @return program parser
     * */
    private static OptionParser getParser() {
        /* parse options */
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("urls", "[REQUIRED] bootstrap URLs")
              .withRequiredArg()
              .describedAs("bootstrap-url")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
        parser.accepts("partitions", "partition-id")
              .withRequiredArg()
              .describedAs("partition-id")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("store", "store name")
              .withRequiredArg()
              .describedAs("store-name")
              .ofType(String.class);
        parser.accepts("verbose", "verbose");
        return parser;
    }

    /**
     * Print Usage to STDOUT
     */
    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("ConsistencyCheck Tool\n  Scan partitions of a store by bootstrap url(s) ");
        help.append("for consistency and optionally print out inconsistent keys\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --partitions <partitionId>[,<partitionId>...]\n");
        help.append("    --urls <url>[,<url>...]\n");
        help.append("    --store <storeName>\n");
        help.append("  Optional:\n");
        help.append("    --verbose\n");
        help.append("    --help\n");
        // TODO: Expand this note to more clearly explain when/why to use
        // multiple. The use case is confirming that clusters replicated via
        // external mechanisms can be checked for consistency.
        // TODO: Do multiple URLs only work if the non-zoned clusters have the
        // exact same number of partitions? If yes, then expand note. If no,
        // then explain how to specify appropriate partitions per url. (I hope
        // the answer is 'yes, tool only works on distinct clusters with exact
        // same # of partitions'.
        help.append("  Note:\n");
        help.append("    When multiple urls are used, the versions are identified by value hashes, instead of VectorClocks\n");
        System.out.print(help.toString());
    }

    /**
     * Determine the consistency level of a key
     * 
     * @param versionNodeSetMap A map that maps version to set of PrefixNodes
     * @param replicationFactor Total replication factor for the set of clusters
     * @return ConsistencyLevel Enum
     */
    public static ConsistencyLevel determineConsistency(Map<Version, Set<PrefixNode>> versionNodeSetMap,
                                                        int replicationFactor) {
        boolean fullyConsistent = true;
        Version latestVersion = null;
        for(Map.Entry<Version, Set<PrefixNode>> versionNodeSetEntry: versionNodeSetMap.entrySet()) {
            Version version = versionNodeSetEntry.getKey();
            if(version instanceof VectorClock) {
                if(latestVersion == null
                   || ((VectorClock) latestVersion).getTimestamp() < ((VectorClock) version).getTimestamp()) {
                    latestVersion = version;
                }
            }
            Set<PrefixNode> nodeSet = versionNodeSetEntry.getValue();
            fullyConsistent = fullyConsistent && (nodeSet.size() == replicationFactor);
        }
        if(fullyConsistent) {
            return ConsistencyLevel.FULL;
        } else {
            // latest write consistent, effectively consistent
            if(latestVersion != null
               && versionNodeSetMap.get(latestVersion).size() == replicationFactor) {
                return ConsistencyLevel.LATEST_CONSISTENT;
            }
            // all other states inconsistent
            return ConsistencyLevel.INCONSISTENT;
        }
    }

    // TODO: (refactor) It is hard to wrap my head around why a subset of
    // methods/inner classes are static and another subset are non-static. Can
    // all the static helper methods move to a ConsistencyCheckUtils.java
    // class/namespace? Or, could the non-static members be refactored as a
    // ConsistencyCheckWorker class?
    /**
     * Determine if a key version is invalid by comparing the version's
     * existance and required writes configuration
     * 
     * @param keyVersionNodeSetMap A map that contains keys mapping to a map
     *        that maps versions to set of PrefixNodes
     * @param requiredWrite Required Write configuration
     */
    public static void cleanIneligibleKeys(Map<ByteArray, Map<Version, Set<PrefixNode>>> keyVersionNodeSetMap,
                                           int requiredWrite) {
        Set<ByteArray> keysToDelete = new HashSet<ByteArray>();
        for(Map.Entry<ByteArray, Map<Version, Set<PrefixNode>>> entry: keyVersionNodeSetMap.entrySet()) {
            Set<Version> versionsToDelete = new HashSet<Version>();

            ByteArray key = entry.getKey();
            Map<Version, Set<PrefixNode>> versionNodeSetMap = entry.getValue();
            // mark version for deletion if not enough writes
            for(Map.Entry<Version, Set<PrefixNode>> versionNodeSetEntry: versionNodeSetMap.entrySet()) {
                Set<PrefixNode> nodeSet = versionNodeSetEntry.getValue();
                if(nodeSet.size() < requiredWrite) {
                    versionsToDelete.add(versionNodeSetEntry.getKey());
                }
            }
            // delete versions
            for(Version v: versionsToDelete) {
                versionNodeSetMap.remove(v);
            }
            // mark key for deletion if no versions left
            if(versionNodeSetMap.size() == 0) {
                keysToDelete.add(key);
            }
        }
        // delete keys
        for(ByteArray k: keysToDelete) {
            keyVersionNodeSetMap.remove(k);
        }
    }

    // TODO: (refactor) Make a ConsistencyCheckCLI class and move main and all
    // argument parsing to that class. That will move some distracting stuff
    // out of this file and follow the pattern of RebalanceCLI and
    // ConsistencyFixCLI.
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        OptionSet options = getParser().parse(args);

        /* validate options */
        boolean verbose = false;
        if(options.hasArgument("help")) {
            printUsage();
            return;
        }
        if(!options.hasArgument("urls") || !options.hasArgument("partitions")
           || !options.hasArgument("store")) {
            printUsage();
            return;
        }
        if(options.has("verbose")) {
            verbose = true;
        }

        List<String> urls = (List<String>) options.valuesOf("urls");
        String storeName = (String) options.valueOf("store");
        List<Integer> partitionIds = (List<Integer>) options.valuesOf("partitions");

        ConsistencyCheckStats globalStats = new ConsistencyCheckStats();
        Map<Integer, ConsistencyCheckStats> partitionStatsMap = new HashMap<Integer, ConsistencyCheckStats>();
        /* scan each partitions */
        for(Integer partitionId: partitionIds) {
            ConsistencyCheck checker = new ConsistencyCheck(urls, storeName, partitionId, verbose);
            checker.connect();
            ConsistencyCheckStats partitionStats = checker.execute();
            partitionStatsMap.put(partitionId, partitionStats);
            globalStats.append(partitionStats);
        }

        /* print stats */
        StringBuilder statsString = new StringBuilder();
        // each partition
        statsString.append("TYPE,Store,ParitionId,KeysConsistent,KeysTotal,Consistency\n");
        for(Map.Entry<Integer, ConsistencyCheckStats> entry: partitionStatsMap.entrySet()) {
            Integer partitionId = entry.getKey();
            ConsistencyCheckStats partitionStats = entry.getValue();
            statsString.append("STATS,");
            statsString.append(storeName + ",");
            statsString.append(partitionId + ",");
            statsString.append(partitionStats.consistentKeys + ",");
            statsString.append(partitionStats.totalKeys + ",");
            statsString.append((double) (partitionStats.consistentKeys)
                               / (double) partitionStats.totalKeys);
            statsString.append("\n");
        }
        // all partitions
        statsString.append("STATS,");
        statsString.append(storeName + ",");
        statsString.append("aggregate,");
        statsString.append(globalStats.consistentKeys + ",");
        statsString.append(globalStats.totalKeys + ",");
        statsString.append((double) (globalStats.consistentKeys) / (double) globalStats.totalKeys);
        statsString.append("\n");

        System.out.println();
        System.out.println(statsString.toString());
    }

    /**
     * Convert a key-version-nodeSet information to string
     * 
     * @param key The key
     * @param versionMap mapping versions to set of PrefixNodes
     * @param storeName store's name
     * @param partitionId partition scanned
     * @return a string that describe the information passed in
     */
    public static String keyVersionToString(ByteArray key,
                                            Map<Version, Set<PrefixNode>> versionMap,
                                            String storeName,
                                            Integer partitionId) {
        StringBuilder record = new StringBuilder();
        for(Map.Entry<Version, Set<PrefixNode>> versionSet: versionMap.entrySet()) {
            Version version = versionSet.getKey();
            Set<PrefixNode> nodeSet = versionSet.getValue();

            record.append("BAD_KEY,");
            record.append(storeName + ",");
            record.append(partitionId + ",");
            record.append(ByteUtils.toHexString(key.get()) + ",");
            record.append(nodeSet.toString().replace(", ", ";") + ",");
            if(version instanceof VectorClock) {
                record.append(((VectorClock) version).getTimestamp() + ",");
                record.append(version.toString()
                                     .replaceAll(", ", ";")
                                     .replaceAll(" ts:[0-9]*", "")
                                     .replaceAll("version\\((.*)\\)", "[$1]"));
            }
            if(version instanceof HashedValue) {
                Integer hashValue = ((HashedValue) version).getValueHash();
                Version realVersion = ((HashedValue) version).getInner();
                record.append(((VectorClock) realVersion).getTimestamp() + ",");
                record.append(realVersion.toString()
                                         .replaceAll(", ", ";")
                                         .replaceAll(" ts:[0-9]*", "")
                                         .replaceAll("version\\((.*)\\)", "[$1],"));
                record.append(hashValue);
            }
            record.append("\n");
        }
        return record.toString();
    }

}
