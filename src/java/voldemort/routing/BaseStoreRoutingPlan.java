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

import java.util.List;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;

/**
 * This class wraps up a Cluster object and a StoreDefinition. The methods are
 * effectively helper or util style methods for querying the routing plan that
 * will be generated for a given routing strategy upon store and cluster
 * topology information.
 */
public class BaseStoreRoutingPlan {

    protected final Cluster cluster;
    protected final StoreDefinition storeDefinition;
    protected final RoutingStrategy routingStrategy;

    public BaseStoreRoutingPlan(Cluster cluster, StoreDefinition storeDefinition) {
        this.cluster = cluster;
        this.storeDefinition = storeDefinition;
        this.routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition,
                                                                                  cluster);
    }

    public Cluster getCluster() {
        return cluster;
    }

    public StoreDefinition getStoreDefinition() {
        return storeDefinition;
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
     * @return zone n-ary level for key hosted on node id in zone id.
     */
    // TODO: add unit test.
    public int getZoneNAry(int zoneId, int nodeId, byte[] key) {
        if(cluster.getNodeById(nodeId).getZoneId() != zoneId) {
            throw new VoldemortException("Node " + nodeId + " is not in zone " + zoneId
                                         + "! The node is in zone "
                                         + cluster.getNodeById(nodeId).getZoneId());
        }

        List<Node> replicatingNodes = this.routingStrategy.routeRequest(key);
        int zoneNAry = -1;
        for(Node node: replicatingNodes) {
            // bump up the replica number once you encounter a node in the given
            // zone
            if(node.getZoneId() == zoneId) {
                zoneNAry++;
            }
            // we are done when we find the given node
            if(node.getId() == nodeId) {
                return zoneNAry;
            }
        }
        if(zoneNAry > -1) {
            throw new VoldemortException("Node " + nodeId + " not a replica for the key "
                                         + ByteUtils.toHexString(key) + " in given zone " + zoneId);
        } else {
            throw new VoldemortException("Could not find any replicas for the key "
                                         + ByteUtils.toHexString(key) + " in given zone " + zoneId);
        }
    }

    /**
     * Given a key and a replica type n (< zone replication factor), figure out
     * the node that contains the key as the nth replica in the given zone.
     * 
     * @param zoneId
     * @param zoneNary
     * @param key
     * @return node id that hosts zone n-ary replica for the key
     */
    // TODO: add unit test.
    public int getNodeIdForZoneNary(int zoneId, int zoneNary, byte[] key) {
        List<Node> replicatingNodes = this.routingStrategy.routeRequest(key);
        int zoneNAry = -1;
        for(Node node: replicatingNodes) {
            // bump up the counter if we encounter a replica in the given zone;
            // return current node if counter now matches requested
            if(node.getZoneId() == zoneId) {
                zoneNAry++;

                if(zoneNAry == zoneNary) {
                    return node.getId();
                }
            }
        }
        if(zoneNAry == -1) {
            throw new VoldemortException("Could not find any replicas for the key "
                                         + ByteUtils.toHexString(key) + " in given zone " + zoneId);
        } else {
            throw new VoldemortException("Could not find " + (zoneNary + 1)
                                         + " replicas for the key " + ByteUtils.toHexString(key)
                                         + " in given zone " + zoneId + ". Only found "
                                         + (zoneNAry + 1));
        }
    }

    /**
     * Determines the list of nodes that the key replicates to
     * 
     * @param key
     * @return list of nodes that key replicates to
     */
    public List<Integer> getReplicationNodeList(final byte[] key) {
        return Utils.nodeListToNodeIdList(this.routingStrategy.routeRequest(key));
    }

}
