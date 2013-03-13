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

package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.FetchPartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StreamingStats.Operation;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.StoreInstance;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;

import com.google.protobuf.Message;

/**
 * FetchEntries fetches and return key/value entry.
 * <p>
 * For performance reason use storageEngine.keys() iterator to filter out
 * unwanted keys and then call storageEngine.get() for valid keys.
 * <p>
 */

public class FetchEntriesStreamRequestHandler extends FetchStreamRequestHandler {

    protected final ClosableIterator<ByteArray> keyIterator;

    public FetchEntriesStreamRequestHandler(FetchPartitionEntriesRequest request,
                                            MetadataStore metadataStore,
                                            ErrorCodeMapper errorCodeMapper,
                                            VoldemortConfig voldemortConfig,
                                            StoreRepository storeRepository,
                                            NetworkClassLoader networkClassLoader) {
        super(request,
              metadataStore,
              errorCodeMapper,
              voldemortConfig,
              storeRepository,
              networkClassLoader,
              Operation.FETCH_ENTRIES);
        this.keyIterator = storageEngine.keys();
        logger.info("Starting fetch entries for store '" + storageEngine.getName()
                    + "' with replica to partition mapping " + replicaToPartitionList);
    }

    @Override
    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        if(!keyIterator.hasNext())
            return StreamRequestHandlerState.COMPLETE;

        long startNs = System.nanoTime();
        ByteArray key = keyIterator.next();
        if(streamStats != null)
            streamStats.reportStreamingScan(operation);

        boolean entryAccepted = false;

        if(!fetchOrphaned) {
            // normal code path
            if(StoreInstance.checkKeyBelongsToPartition(nodeId,
                                                        key.get(),
                                                        replicaToPartitionList,
                                                        initialCluster,
                                                        storeDef)) {
                entryAccepted = true;
            }
        } else {
            // orphaned fetches
            if(!StoreInstance.checkKeyBelongsToNode(key.get(), nodeId, initialCluster, storeDef)) {
                entryAccepted = true;
            }
        }

        if(entryAccepted) {
            List<Versioned<byte[]>> values = storageEngine.get(key, null);
            if(streamStats != null)
                streamStats.reportStorageTime(operation, System.nanoTime() - startNs);
            for(Versioned<byte[]> value: values) {
                throttler.maybeThrottle(key.length());
                if(filter.accept(key, value)) {
                    fetched++;
                    if(streamStats != null)
                        streamStats.reportStreamingFetch(operation);
                    VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();

                    VAdminProto.PartitionEntry partitionEntry = VAdminProto.PartitionEntry.newBuilder()
                                                                                          .setKey(ProtoUtils.encodeBytes(key))
                                                                                          .setVersioned(ProtoUtils.encodeVersioned(value))
                                                                                          .build();
                    response.setPartitionEntry(partitionEntry);

                    Message message = response.build();

                    startNs = System.nanoTime();
                    ProtoUtils.writeMessage(outputStream, message);
                    if(streamStats != null)
                        streamStats.reportNetworkTime(operation, System.nanoTime() - startNs);

                    throttler.maybeThrottle(AdminServiceRequestHandler.valueSize(value));
                }
            }
        } else {
            if(streamStats != null)
                streamStats.reportStorageTime(operation, System.nanoTime() - startNs);
        }

        // log progress
        counter++;

        if(0 == counter % STAT_RECORDS_INTERVAL) {
            long totalTime = (System.currentTimeMillis() - startTime) / Time.MS_PER_SECOND;

            logger.info("Fetch entries scanned " + counter + " entries, fetched " + fetched
                        + " entries for store '" + storageEngine.getName()
                        + "' replicaToPartitionList:" + replicaToPartitionList + " in " + totalTime
                        + " s");
        }

        // TODO: Add logic to FetchKeys and FetchEntries to count keys per
        // partition correctly.

        if(keyIterator.hasNext() && counter < recordsPerPartition)
            return StreamRequestHandlerState.WRITING;
        else {
            return StreamRequestHandlerState.COMPLETE;
        }
    }

    @Override
    public final void close(DataOutputStream outputStream) throws IOException {
        if(null != keyIterator)
            keyIterator.close();
        super.close(outputStream);
    }
}
