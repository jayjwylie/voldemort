package voldemort.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.store.routed.NodeValue;
import voldemort.store.routed.ReadRepairer;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class ConsistencyFix {

    private static class VoldemortInstance {

        private final Cluster cluster;
        private StoreDefinition storeDefinition;
        private final AdminClient adminClient;
        private final Map<Integer, Integer> partitionIdToNodeIdMap;

        public VoldemortInstance(String url, String storeName) throws Exception {
            System.out.println("Connecting to bootstrap server: " + url);
            adminClient = new AdminClient(url, new AdminClientConfig(), 0);
            cluster = adminClient.getAdminClientCluster();
            System.out.println("Cluster determined to be: " + cluster.getName());

            System.out.println("Determining store definition for store: " + storeName);
            Versioned<List<StoreDefinition>> storeDefinitions = adminClient.getRemoteStoreDefList(0);
            List<StoreDefinition> StoreDefitions = storeDefinitions.getValue();
            boolean storeFound = false;
            for(StoreDefinition def: StoreDefitions) {
                if(def.getName().equals(storeName)) {
                    storeDefinition = def;
                    storeFound = true;
                    break;
                }
            }
            if(!storeFound) {
                throw new Exception("Store definition for store '" + storeName + "' not found.");
            }
            System.out.println("Store definition determined.");

            System.out.println("Determining partition ID to node ID mapping.");
            partitionIdToNodeIdMap = RebalanceUtils.getCurrentPartitionMapping(cluster);
        }

        public Cluster getCluster() {
            return cluster;
        }

        public StoreDefinition getStoreDefinition() {
            return storeDefinition;
        }

        public String getStoreName() {
            return storeDefinition.getName();
        }

        public AdminClient getAdminClient() {
            return adminClient;
        }

        public Map<Integer, Integer> getPartitionIdToNodeIdMap() {
            return partitionIdToNodeIdMap;
        }

        public List<Integer> getReplicationPartitionList(int partitionId) {
            return new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition, cluster)
                                               .getReplicatingPartitionList(partitionId);
        }

        public int getMasterPartitionId(String keyInHexFormat) throws DecoderException {
            byte[] key = Hex.decodeHex(keyInHexFormat.toCharArray());
            return new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition, cluster)
                                               .getMasterPartition(key);
        }

        public int getNodeIdForPartitionId(int partitionId) {
            return partitionIdToNodeIdMap.get(partitionId);
        }

        // Throws exception if duplicate nodes are found. I.e., partition list
        // is assumed to be "replicating" partition list.
        private List<Integer> getNodeIdListForPartitionIdList(List<Integer> partitionIds)
                throws Exception {
            List<Integer> nodeIds = new ArrayList<Integer>(partitionIds.size());
            for(Integer partitionId: partitionIds) {
                int nodeId = getNodeIdForPartitionId(partitionId);
                if(nodeIds.contains(nodeId)) {
                    throw new Exception("Node ID " + nodeId + " already in list of Node IDs.");
                } else {
                    nodeIds.add(nodeId);
                }
            }
            return nodeIds;
        }

        public List<Integer> getReplicationNodeList(int partitionId) throws Exception {
            return getNodeIdListForPartitionIdList(getReplicationPartitionList(partitionId));
        }
    }

    public static void main(String[] args) throws Exception {
        /* parse options */
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url")
              .withRequiredArg()
              .describedAs("The bootstrap url.")
              .ofType(String.class);
        parser.accepts("store")
              .withRequiredArg()
              .describedAs("The store name.")
              .ofType(String.class);
        parser.accepts("key")
              .withRequiredArg()
              .describedAs("The key in hexadecimal format.")
              .ofType(String.class);
        parser.accepts("keys")
              .withRequiredArg()
              .withValuesSeparatedBy(',')
              .describedAs("List of keys. "
                           + "Each key must be in hexadecimal format. "
                           + "Each key must be separated only by a comma ',' without any white space.")
              .ofType(String.class);
        parser.accepts("key-file")
              .withRequiredArg()
              .describedAs("Name of key-file. " + "Each key must be in hexadecimal format. "
                           + "Each key must be on a separate line in the file. ")
              .ofType(String.class);

        parser.accepts("verbose", "verbose");
        OptionSet options = parser.parse(args);

        /* validate options */
        if(options.hasArgument("help")) {
            parser.printHelpOn(System.out);
            printUsage();
            return;
        }
        if(!options.hasArgument("url")) {
            printUsage("Missing required 'url' argument.");
            return;
        }
        if(!options.hasArgument("store")) {
            printUsage("Missing required 'store' argument.");
            return;
        }
        if(!options.has("key") && !options.has("keys") && !options.has("key-file")) {
            printUsage("Missing required key-specifying argument: 'key', 'keys', or 'key-file'.");
            return;
        }
        if((options.has("key") && options.has("keys"))
           || (options.has("key") && options.has("key-file"))
           || (options.has("keys") && options.has("key-file"))) {
            printUsage("Please provide exactly one key-specifying argument: 'key', 'keys', or 'key-file'.");
            return;
        }

        boolean verbose = false;
        if(options.has("verbose")) {
            verbose = true;
        }

        String url = (String) options.valueOf("url");
        String storeName = (String) options.valueOf("store");

        List<String> keysInHexFormat = new LinkedList<String>();
        if(options.has("key")) {
            keysInHexFormat.add((String) options.valueOf("key"));
        }
        if(options.has("keys")) {
            @SuppressWarnings("unchecked")
            List<String> valuesOf = (List<String>) options.valuesOf("keys");
            keysInHexFormat = valuesOf;
        }
        if(options.has("key-file")) {
            String keyFile = (String) options.valueOf("key-file");
            System.err.println("Key file: " + keyFile);
            try {
                BufferedReader fileReader = new BufferedReader(new FileReader(keyFile));
                for(String line = fileReader.readLine(); line != null; line = fileReader.readLine()) {
                    if(!line.isEmpty()) {
                        keysInHexFormat.add(line);
                    }
                }
            } catch(IOException e) {
                Utils.croak("Failure to open input stream: " + e.getMessage());
            }
        }
        // TODO: Make printing out keys part of verbose aspect of fixKey
        /*-
        for(String keyInHexFormat: keysInHexFormat) {
            System.out.println("<<" + keyInHexFormat + ">>");
        }
         */
        VoldemortInstance vInstance = new VoldemortInstance(url, storeName);
        for(String keyInHexFormat: keysInHexFormat) {
            fixKey(vInstance, keyInHexFormat, verbose);
        }
        // TODO: Add functionality to output two files:
        // - list of keys successfully fixed. This is a nicety and may not be
        // worth the effort. Or, maybe just verbose output is sufficient for
        // this.
        // - list of keys not yet fixed. This one should be in same format as
        // key-file input so that it could just be re-run. This is the important
        // output file.

    }

    public static void printUsage() {
        System.out.println("Required arguments: \n" + " --url <url>\n" + " --store <storeName>\n"
                           + " (--key <keyInHexFormat> | --keys <keysInHexFormatSeparatedByComma "
                           + "| --key-file <FileNameOfListOfKeys>)\n");
    }

    public static void printUsage(String errMessage) {
        System.err.println("Error: " + errMessage);
        printUsage();
    }

    // TODO: actually use 'verbose' flag to modulate output
    // TODO: iterable set of keys, rather than single String keyInHexFormat?
    public static void fixKey(VoldemortInstance vInstance, String keyInHexFormat, boolean verbose)
            throws Exception {
        int masterPartitionId = vInstance.getMasterPartitionId(keyInHexFormat);
        List<Integer> nodeIdList = vInstance.getReplicationNodeList(masterPartitionId);

        byte[] key = ByteUtils.fromHexString(keyInHexFormat);
        List<ByteArray> keys = new ArrayList<ByteArray>();
        keys.add(new ByteArray(key));

        // TODO: Refactor st 'READ', 'RESOLVE CONFLICTS', and 'WRITE' phases are
        // separate methods.

        // *************** READ *********************

        // TODO: The type returned by queryKeys is *messy*. A type of
        // {ByteArray, List<Versioned<byte[]>, Exception} needs to be defined.
        // And, Versioned<byte[]> may also warrant its own type.
        System.out.println("Reading key-values for specified key: " + keyInHexFormat);
        Map<Integer, Iterator<Pair<ByteArray, Pair<List<Versioned<byte[]>>, Exception>>>> nodeIdToKeyValues;
        nodeIdToKeyValues = new HashMap<Integer, Iterator<Pair<ByteArray, Pair<List<Versioned<byte[]>>, Exception>>>>();
        for(int nodeId: nodeIdList) {
            Iterator<Pair<ByteArray, Pair<List<Versioned<byte[]>>, Exception>>> keyValues;
            keyValues = vInstance.getAdminClient().queryKeys(nodeId,
                                                             vInstance.getStoreName(),
                                                             keys.iterator());
            nodeIdToKeyValues.put(nodeId, keyValues);
        }

        System.out.println("Confirming all nodes (" + nodeIdList
                           + ") responded with key-values for specified key: " + keyInHexFormat);
        List<NodeValue<ByteArray, byte[]>> nodeValues = Lists.newArrayList();
        boolean exceptionsEncountered = false;
        for(int nodeId: nodeIdList) {
            System.out.println("\t Processing response from node with id:" + nodeId);
            Pair<ByteArray, Pair<List<Versioned<byte[]>>, Exception>> keyValue;
            if(nodeIdToKeyValues.get(nodeId).hasNext()) {
                System.out.println("\t... There was a key-value returned from node with id:"
                                   + nodeId);
                keyValue = nodeIdToKeyValues.get(nodeId).next();

                Exception e = keyValue.getSecond().getSecond();
                if(e != null) {
                    System.out.println("\t... Exception encountered while fetching key "
                                       + keyInHexFormat + " from node with nodeId " + nodeId
                                       + " : " + e.getMessage());
                    exceptionsEncountered = true;
                } else {
                    ByteArray keyByteArray = keyValue.getFirst();
                    List<Versioned<byte[]>> values = keyValue.getSecond().getFirst();
                    if(values.isEmpty()) {
                        System.out.println("\t... Adding null version to nodeValues");
                        Versioned<byte[]> versioned = new Versioned<byte[]>(null);
                        nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId,
                                                                        new ByteArray(key),
                                                                        versioned));

                    } else {
                        for(Versioned<byte[]> value: values) {
                            System.out.println("\t... Adding following version to nodeValues: "
                                               + value.getVersion());
                            nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId,
                                                                            keyByteArray,
                                                                            value));
                        }
                    }
                }
            } else {
                System.out.println("\t... No key-value returned from node with id:" + nodeId);
                System.out.println("\t... Adding null version to nodeValues");
                Versioned<byte[]> versioned = new Versioned<byte[]>(null);
                nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId,
                                                                new ByteArray(key),
                                                                versioned));
            }
        }
        if(exceptionsEncountered) {
            System.err.println("Aborting fixKey because exceptions were encountered when fetching key-values.");
            return;
        }

        // *************** RESOLVE CONFLICTS *********************
        // Decide on the specific key-value to write everywhere.
        // Some cut-paste-and-modify coding from AbstractReadRepair.java...
        System.out.println("Resolving conflicts in responses.");

        ReadRepairer<ByteArray, byte[]> readRepairer = new ReadRepairer<ByteArray, byte[]>();
        List<NodeValue<ByteArray, byte[]>> toReadRepair = Lists.newArrayList();
        /*
         * We clone after computing read repairs in the assumption that the
         * output will be smaller than the input. Note that we clone the
         * version, but not the key or value as the latter two are not mutated.
         */
        for(NodeValue<ByteArray, byte[]> v: readRepairer.getRepairs(nodeValues)) {
            Versioned<byte[]> versioned = Versioned.value(v.getVersioned().getValue(),
                                                          ((VectorClock) v.getVersion()).clone());
            System.out.println("\tAdding toReadRepair: key (" + v.getKey() + "), version ("
                               + versioned.getVersion() + ")");
            toReadRepair.add(new NodeValue<ByteArray, byte[]>(v.getNodeId(), v.getKey(), versioned));
        }

        // *************** WRITE *********************
        // TODO: do streaming repairs. See updateEntries of AdminClient and
        // DonorBasedRebalancePusherSlave for ideas.

        System.out.println("Repair work to be done:");
        for(NodeValue<ByteArray, byte[]> nodeKeyValue: toReadRepair) {
            System.out.println("\tRepair key " + nodeKeyValue.getKey() + "on node with id "
                               + nodeKeyValue.getNodeId() + " for version "
                               + nodeKeyValue.getVersion());
        }

        System.out.println("Performing repair work:");
        boolean allRepairsSuccessful = true;
        for(NodeValue<ByteArray, byte[]> nodeKeyValue: toReadRepair) {
            System.out.println("\tDoing repair for node with id:" + nodeKeyValue.getNodeId());
            Exception e = vInstance.getAdminClient().repairEntry(vInstance.getStoreName(),
                                                                 nodeKeyValue);
            if(e != null) {
                System.out.println("\t... Repair of key " + nodeKeyValue.getKey()
                                   + "on node with id " + nodeKeyValue.getNodeId()
                                   + " for version " + nodeKeyValue.getVersion()
                                   + " failed because of exception : " + e.getMessage());
                allRepairsSuccessful = false;
            }
        }
        if(!allRepairsSuccessful) {
            System.err.println("Aborting fixKey because exceptions were encountered when reparing key-values.");
            System.out.println("Fix failed...");
            return;
        }
        System.out.println("Fix completed successfully!!");
        return;
    }
}
