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

package voldemort.performance;

import java.io.File;
import java.util.List;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.InvalidMetadataException;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

public class ProfileRouteRequest {

    public static void profileRouting(RoutingStrategy strategy,
                                      Cluster cluster,
                                      StoreDefinition storeDef) {
        long totalTimeNs = 0;
        long totalOps = 100000;

        System.out.println("\nProfiling for store " + storeDef.getName());
        Node node = cluster.getNodeById(0);

        for(int i = 0; i < totalOps; i++) {
            ByteArray key = new ByteArray(("key" + i).getBytes());
            long startNs = System.nanoTime();
            try {
                StoreUtils.assertValidMetadata(key, strategy, node);
            } catch(InvalidMetadataException ive) {
                // eat this
            }
            long stopNs = System.nanoTime();
            // System.out.println(stopNs - startNs);
            totalTimeNs += stopNs - startNs;
        }
        System.out.println("Avg (ns) :" + (totalTimeNs / totalOps));
    }

    public static void profileAllStores(Cluster cluster, List<StoreDefinition> storeDefList) {
        RoutingStrategyFactory rFactory = new RoutingStrategyFactory();
        for(StoreDefinition storeDef: storeDefList) {
            RoutingStrategy strategy = rFactory.updateRoutingStrategy(storeDef, cluster);
            profileRouting(strategy, cluster, storeDef);
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        ClusterMapper cmapper = new ClusterMapper();
        StoreDefinitionsMapper smapper = new StoreDefinitionsMapper();
        String clusterName = "cnc-02/";
        // String clusterName = "news/";
        // String clusterName = "money-01/";
        String JayPath = "/home/jwylie/projects/li/svn/voldemort_clusters/prod/" + clusterName;
        String VinothPath = "/home/vchandar/cache/zone-expansion-test/cnc02-hit/";

        Cluster baseCluster = cmapper.readCluster(new File(JayPath + "cluster.xml"));
        Cluster interimCluster = cmapper.readCluster(new File(JayPath + "interim-cluster.xml"));
        Cluster finalCluster = cmapper.readCluster(new File(JayPath + "final-cluster.xml"));
        List<StoreDefinition> baseStoreDefs = smapper.readStoreList(new File(JayPath + "stores.xml"));
        List<StoreDefinition> finalStoreDefs = smapper.readStoreList(new File(JayPath
                                                                              + "final-stores.xml"));
        System.out.println("\n\nFinal Cluster/Stores");
        profileAllStores(finalCluster, finalStoreDefs);

        System.out.println("\n\nBase Cluster/Stores");
        profileAllStores(baseCluster, baseStoreDefs);

        System.out.println("\n\nInterim Cluster/Stores");
        profileAllStores(interimCluster, finalStoreDefs);
    }
}