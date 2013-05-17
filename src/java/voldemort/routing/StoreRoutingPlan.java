/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClusterUtils;
import voldemort.utils.NodeUtils;
import voldemort.utils.Pair;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;

// TODO: Add StoreInstanceTest unit test for these helper methods.

/**
 * This class wraps up a Cluster object and a StoreDefinition. The methods are
 * effectively helper or util style methods for querying the routing plan that
 * will be generated for a given routing strategy upon store and cluster
 * topology information.
 */
public class StoreRoutingPlan {

    private final Cluster cluster;
    private final StoreDefinition storeDefinition;
    private final Map<Integer, Integer> partitionIdToNodeIdMap;
    private final Map<Integer, List<Integer>> nodeIdToNaryPartitionMap;
    private final RoutingStrategy routingStrategy;

    public StoreRoutingPlan(Cluster cluster, StoreDefinition storeDefinition) {
        this.cluster = cluster;
        this.storeDefinition = storeDefinition;
        this.partitionIdToNodeIdMap = ClusterUtils.getCurrentPartitionMapping(cluster);
        this.routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition,
                                                                                  cluster);
        this.nodeIdToNaryPartitionMap = new HashMap<Integer, List<Integer>>();
        for(int nodeId: cluster.getNodeIds()) {
            this.nodeIdToNaryPartitionMap.put(nodeId, new ArrayList<Integer>());
        }
        for(int masterPartitionId = 0; masterPartitionId < cluster.getNumberOfPartitions(); ++masterPartitionId) {
            List<Integer> naryPartitionIds = getReplicatingPartitionList(masterPartitionId);
            for(int naryPartitionId: naryPartitionIds) {
                int naryNodeId = getNodeIdForPartitionId(naryPartitionId);
                nodeIdToNaryPartitionMap.get(naryNodeId).add(masterPartitionId);
            }
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public StoreDefinition getStoreDefinition() {
        return storeDefinition;
    }

    /**
     * Determines list of partition IDs that replicate the master partition ID.
     * 
     * @param masterPartitionId
     * @return List of partition IDs that replicate the master partition ID.
     */
    public List<Integer> getReplicatingPartitionList(int masterPartitionId) {
        return this.routingStrategy.getReplicatingPartitionList(masterPartitionId);
    }

    // TODO: Add test for this method (if this method is still required after
    // the RebalanceController is updated to use RebalancePlan).
    /**
     * 
     * @param nodeId
     * @return all nary partition IDs hosted on the node.
     */
    public List<Integer> getNaryPartitionIds(int nodeId) {
        return nodeIdToNaryPartitionMap.get(nodeId);
    }

    /**
     * Determines list of partition IDs that replicate the key.
     * 
     * @param key
     * @return List of partition IDs that replicate the given key
     */
    public List<Integer> getReplicatingPartitionList(final byte[] key) {
        return this.routingStrategy.getPartitionList(key);
    }

    /**
     * Determines the list of nodes that the key replicates to
     * 
     * @param key
     * @return list of nodes that key replicates to
     */
    public List<Integer> getReplicationNodeList(final byte[] key) {
        return NodeUtils.getNodeIds(this.routingStrategy.routeRequest(key));
    }

    /**
     * Determines master partition ID for the key.
     * 
     * @param key
     * @return
     */
    public int getMasterPartitionId(final byte[] key) {
        return this.routingStrategy.getMasterPartition(key);
    }

    /**
     * Determines node ID that hosts the specified partition ID.
     * 
     * @param partitionId
     * @return
     */
    public int getNodeIdForPartitionId(int partitionId) {
        return partitionIdToNodeIdMap.get(partitionId);
    }

    /**
     * Determines the partition ID that replicates the key on the given node.
     * 
     * @param nodeId of the node
     * @param key to look up.
     * @return partitionId if found, otherwise null.
     */
    public Integer getNodesPartitionIdForKey(int nodeId, final byte[] key) {
        // this is all the partitions the key replicates to.
        List<Integer> partitionIds = getReplicatingPartitionList(key);
        for(Integer partitionId: partitionIds) {
            // check which of the replicating partitions belongs to the node in
            // question
            if(getNodeIdForPartitionId(partitionId) == nodeId) {
                return partitionId;
            }
        }
        return null;
    }

    /**
     * Converts from partitionId to nodeId. The list of partition IDs,
     * partitionIds, is expected to be a "replicating partition list", i.e., the
     * mapping from partition ID to node ID should be one to one.
     * 
     * @param partitionIds List of partition IDs for which to find the Node ID
     *        for the Node that owns the partition.
     * @return List of node ids, one for each partition ID in partitionIds
     * @throws VoldemortException If multiple partition IDs in partitionIds map
     *         to the same Node ID.
     */
    private List<Integer> getNodeIdListForPartitionIdList(List<Integer> partitionIds)
            throws VoldemortException {
        List<Integer> nodeIds = new ArrayList<Integer>(partitionIds.size());
        for(Integer partitionId: partitionIds) {
            int nodeId = getNodeIdForPartitionId(partitionId);
            if(nodeIds.contains(nodeId)) {
                throw new VoldemortException("Node ID " + nodeId + " already in list of Node IDs.");
            } else {
                nodeIds.add(nodeId);
            }
        }
        return nodeIds;
    }

    /**
     * Returns the list of node ids this partition replicates to.
     * 
     * TODO ideally the {@link RoutingStrategy} should house a routeRequest(int
     * partition) method
     * 
     * @param partitionId
     * @return
     * @throws VoldemortException
     */
    public List<Integer> getReplicationNodeList(int partitionId) throws VoldemortException {
        return getNodeIdListForPartitionIdList(getReplicatingPartitionList(partitionId));
    }

    /**
     * Given a key that belong to a given node, returns a number n (< zone
     * replication factor), such that the given node holds the key as the nth
     * replica of the given zone
     * 
     * eg: if the method returns 1, then given node hosts the key as the zone
     * secondary in the given zone
     * 
     * @param zoneId
     * @param nodeId
     * @param key
     * @return
     */
    public int getZoneReplicaType(int zoneId, int nodeId, byte[] key) {
        if(cluster.getNodeById(nodeId).getZoneId() != zoneId) {
            throw new VoldemortException("Node " + nodeId + " is not in zone " + zoneId
                                         + "! The node is in zone "
                                         + cluster.getNodeById(nodeId).getZoneId());
        }

        List<Node> replicatingNodes = this.routingStrategy.routeRequest(key);
        int zoneReplicaType = -1;
        for(Node node: replicatingNodes) {
            // bump up the replica number once you encounter a node in the given
            // zone
            if(node.getZoneId() == zoneId) {
                zoneReplicaType++;
            }
            // we are done when we find the given node
            if(node.getId() == nodeId) {
                return zoneReplicaType;
            }
        }
        if(zoneReplicaType > -1) {
            throw new VoldemortException("Node " + nodeId + " not a replica for the key "
                                         + ByteUtils.toHexString(key) + " in given zone " + zoneId);
        } else {
            throw new VoldemortException("Could not find any replicas for the key "
                                         + ByteUtils.toHexString(key) + " in given zone " + zoneId);
        }
    }

    // TODO: After other rebalancing code is cleaned up, either document and add
    // a test, or remove this method. (Unclear if this method is needed once we
    // drop replicaType from some key code paths).
    public boolean hasZoneReplicaType(int zoneId, int partitionId) {
        List<Integer> replicatingNodeIds = getReplicationNodeList(partitionId);
        for(int replicatingNodeId: replicatingNodeIds) {
            Node replicatingNode = cluster.getNodeById(replicatingNodeId);
            // bump up the replica number once you encounter a node in the given
            // zone
            if(replicatingNode.getZoneId() == zoneId) {
                return true;
            }
        }
        return false;
    }

    // TODO: After other rebalancing code is cleaned up, either document and add
    // a test, or remove this method. (Unclear if this method is needed once we
    // drop replicaType from some key code paths).
    /**
     * 
     * @param zoneId
     * @param nodeId
     * @param partitionId
     */
    public int getZoneReplicaType(int zoneId, int nodeId, int partitionId) {
        if(cluster.getNodeById(nodeId).getZoneId() != zoneId) {
            throw new VoldemortException("Node " + nodeId + " is not in zone " + zoneId
                                         + "! The node is in zone "
                                         + cluster.getNodeById(nodeId).getZoneId());
        }

        List<Integer> replicatingNodeIds = getReplicationNodeList(partitionId);
        int zoneReplicaType = -1;
        for(int replicatingNodeId: replicatingNodeIds) {
            Node replicatingNode = cluster.getNodeById(replicatingNodeId);
            // bump up the replica number once you encounter a node in the given
            // zone
            if(replicatingNode.getZoneId() == zoneId) {
                zoneReplicaType++;
            }
            if(replicatingNode.getId() == nodeId) {
                return zoneReplicaType;
            }
        }
        if(zoneReplicaType > 0) {
            throw new VoldemortException("Node " + nodeId + " not a replica for partition "
                                         + partitionId + " in given zone " + zoneId);
        } else {
            throw new VoldemortException("Could not find any replicas for partition Id "
                                         + partitionId + " in given zone " + zoneId);
        }
    }

    // TODO: After other rebalancing code is cleaned up, either document and add
    // a test, or remove this method. (Unclear if this method is needed once we
    // drop replicaType from some key code paths).
    /**
     * 
     * @param nodeId
     * @param partitionId
     */
    public int getReplicaType(int nodeId, int partitionId) {
        List<Integer> replicatingNodeIds = getReplicationNodeList(partitionId);
        int replicaType = -1;
        for(int replicatingNodeId: replicatingNodeIds) {
            Node replicatingNode = cluster.getNodeById(replicatingNodeId);
            replicaType++;
            if(replicatingNode.getId() == nodeId) {
                return replicaType;
            }
        }
        if(replicaType > 0) {
            throw new VoldemortException("Node " + nodeId + " not a replica for partition "
                                         + partitionId);
        } else {
            throw new VoldemortException("Could not find any replicas for partition Id "
                                         + partitionId);
        }
    }

    /**
     * Given a key and a replica type n (< zone replication factor), figure out
     * the node that contains the key as the nth replica in the given zone.
     * 
     * @param zoneId
     * @param zoneReplicaType
     * @param key
     * @return
     */
    public int getZoneReplicaNode(int zoneId, int zoneReplicaType, byte[] key) {
        List<Node> replicatingNodes = this.routingStrategy.routeRequest(key);
        int zoneReplicaTypeCounter = -1;
        for(Node node: replicatingNodes) {
            // bump up the counter if we encounter a replica in the given zone
            if(node.getZoneId() == zoneId) {
                zoneReplicaTypeCounter++;
            }
            // when the counter matches up with the replicaNumber we need, we
            // are done.
            if(zoneReplicaTypeCounter == zoneReplicaType) {
                return node.getId();
            }
        }
        if(zoneReplicaTypeCounter == -1) {
            throw new VoldemortException("Could not find any replicas for the key "
                                         + ByteUtils.toHexString(key) + " in given zone " + zoneId);
        } else {
            throw new VoldemortException("Could not find " + (zoneReplicaType + 1)
                                         + " replicas for the key " + ByteUtils.toHexString(key)
                                         + " in given zone " + zoneId + ". Only found "
                                         + (zoneReplicaTypeCounter + 1));
        }
    }

    // TODO: After other rebalancing code is cleaned up, either document and add
    // a test, or remove this method. (Unclear if this method is needed once we
    // drop replicaType from some key code paths).
    public int getZoneReplicaNodeId(int zoneId, int zoneReplicaType, int partitionId) {
        List<Integer> replicatingNodeIds = getReplicationNodeList(partitionId);

        int zoneReplicaTypeCounter = -1;
        for(int replicatingNodeId: replicatingNodeIds) {
            Node replicatingNode = cluster.getNodeById(replicatingNodeId);
            // bump up the counter if we encounter a replica in the given zone
            if(replicatingNode.getZoneId() == zoneId) {
                zoneReplicaTypeCounter++;
            }
            // when the counter matches up with the replicaNumber we need, we
            // are done.
            if(zoneReplicaTypeCounter == zoneReplicaType) {
                return replicatingNode.getId();
            }
        }
        if(zoneReplicaTypeCounter == 0) {
            throw new VoldemortException("Could not find any replicas for the partition "
                                         + partitionId + " in given zone " + zoneId);
        } else {
            throw new VoldemortException("Could not find " + zoneReplicaType
                                         + " replicas for the partition " + partitionId
                                         + " in given zone " + zoneId + ". Only found "
                                         + zoneReplicaTypeCounter);
        }
    }

    // TODO: (refactor) Move from static methods to non-static methods that use
    // this object's cluster and storeDefinition member for the various
    // check*BelongsTo* methods.

    /**
     * Check that the key belongs to one of the partitions in the map of replica
     * type to partitions
     * 
     * @param keyPartitions Preference list of the key
     * @param nodePartitions Partition list on this node
     * @param replicaToPartitionList Mapping of replica type to partition list
     * @return Returns a boolean to indicate if this belongs to the map
     */
    public static boolean checkKeyBelongsToPartition(List<Integer> keyPartitions,
                                                     List<Integer> nodePartitions,
                                                     HashMap<Integer, List<Integer>> replicaToPartitionList) {
        // Check for null
        replicaToPartitionList = Utils.notNull(replicaToPartitionList);

        for(int replicaNum = 0; replicaNum < keyPartitions.size(); replicaNum++) {

            // If this partition belongs to node partitions + master is in
            // replicaToPartitions list -> match
            if(nodePartitions.contains(keyPartitions.get(replicaNum))) {
                List<Integer> partitionsToMove = replicaToPartitionList.get(replicaNum);
                if(partitionsToMove != null && partitionsToMove.size() > 0) {
                    if(partitionsToMove.contains(keyPartitions.get(0))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Check that the key belongs to one of the partitions in the map of replica
     * type to partitions
     * 
     * @param nodeId Node on which this is running ( generally stealer node )
     * @param key The key to check
     * @param replicaToPartitionList Mapping of replica type to partition list
     * @param cluster Cluster metadata
     * @param storeDef The store definition
     * @return Returns a boolean to indicate if this belongs to the map
     */
    public static boolean checkKeyBelongsToPartition(int nodeId,
                                                     byte[] key,
                                                     HashMap<Integer, List<Integer>> replicaToPartitionList,
                                                     Cluster cluster,
                                                     StoreDefinition storeDef) {
        boolean checkResult = false;
        if(storeDef.getRoutingStrategyType().equals(RoutingStrategyType.TO_ALL_STRATEGY)
           || storeDef.getRoutingStrategyType()
                      .equals(RoutingStrategyType.TO_ALL_LOCAL_PREF_STRATEGY)) {
            checkResult = true;
        } else {
            List<Integer> keyPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                             cluster)
                                                                      .getPartitionList(key);
            List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
            checkResult = StoreRoutingPlan.checkKeyBelongsToPartition(keyPartitions,
                                                                      nodePartitions,
                                                                      replicaToPartitionList);
        }
        return checkResult;
    }

    /***
     * 
     * @return true if the partition belongs to the node with given replicatype
     */
    public static boolean checkPartitionBelongsToNode(int partitionId,
                                                      int replicaType,
                                                      int nodeId,
                                                      Cluster cluster,
                                                      StoreDefinition storeDef) {
        boolean belongs = false;
        List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
        List<Integer> replicatingPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster)
                                                                          .getReplicatingPartitionList(partitionId);
        // validate replicaType
        if(replicaType < replicatingPartitions.size()) {
            // check if the replicaType'th partition in the replicating list,
            // belongs to the given node
            if(nodePartitions.contains(replicatingPartitions.get(replicaType)))
                belongs = true;
        }

        return belongs;
    }

    /**
     * Given a key and a list of steal infos give back a list of stealer node
     * ids which will steal this.
     * 
     * @param key Byte array of key
     * @param stealerNodeToMappingTuples Pairs of stealer node id to their
     *        corresponding [ partition - replica ] tuples
     * @param cluster Cluster metadata
     * @param storeDef Store definitions
     * @return List of node ids
     */
    public static List<Integer> checkKeyBelongsToPartition(byte[] key,
                                                           Set<Pair<Integer, HashMap<Integer, List<Integer>>>> stealerNodeToMappingTuples,
                                                           Cluster cluster,
                                                           StoreDefinition storeDef) {
        List<Integer> keyPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                         cluster)
                                                                  .getPartitionList(key);
        List<Integer> nodesToPush = Lists.newArrayList();
        for(Pair<Integer, HashMap<Integer, List<Integer>>> stealNodeToMap: stealerNodeToMappingTuples) {
            List<Integer> nodePartitions = cluster.getNodeById(stealNodeToMap.getFirst())
                                                  .getPartitionIds();
            if(StoreRoutingPlan.checkKeyBelongsToPartition(keyPartitions,
                                                           nodePartitions,
                                                           stealNodeToMap.getSecond())) {
                nodesToPush.add(stealNodeToMap.getFirst());
            }
        }
        return nodesToPush;
    }

    /***
     * Checks if a given partition is stored in the node. (It can be primary or
     * a secondary)
     * 
     * @param partition
     * @param nodeId
     * @param cluster
     * @param storeDef
     * @return
     */
    public static boolean checkPartitionBelongsToNode(int partition,
                                                      int nodeId,
                                                      Cluster cluster,
                                                      StoreDefinition storeDef) {
        List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
        List<Integer> replicatingPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster)
                                                                          .getReplicatingPartitionList(partition);
        // remove all partitions from the list, except those that belong to the
        // node
        replicatingPartitions.retainAll(nodePartitions);
        return replicatingPartitions.size() > 0;
    }

    /**
     * 
     * @param key
     * @param nodeId
     * @param cluster
     * @param storeDef
     * @return true if the key belongs to the node as some replica
     */
    public static boolean checkKeyBelongsToNode(byte[] key,
                                                int nodeId,
                                                Cluster cluster,
                                                StoreDefinition storeDef) {
        List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
        List<Integer> replicatingPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster)
                                                                          .getPartitionList(key);
        // remove all partitions from the list, except those that belong to the
        // node
        replicatingPartitions.retainAll(nodePartitions);
        return replicatingPartitions.size() > 0;
    }

}
