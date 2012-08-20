/*
 * Copyright 2008-2012 LinkedIn, Inc
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

package voldemort.store.socket.clientrequest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.RequestRoutingType;
import voldemort.store.StoreTimeoutException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.stats.ClientSocketStats;
import voldemort.store.stats.ClientSocketStatsJmx;
import voldemort.utils.JmxUtils;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.utils.pool.KeyedResourcePool;
import voldemort.utils.pool.ResourcePoolConfig;

// TODO: Update class header once the updates to the non-blocking API is
// complete.
/**
 * A pool of {@link ClientRequestExecutor} keyed off the
 * {@link SocketDestination}. This is a wrapper around {@link KeyedResourcePool}
 * that translates exceptions as well as providing some JMX access.
 * 
 * <p/>
 * 
 * Upon successful construction of this object, a new Thread is started. It is
 * terminated upon calling {@link #close()}.
 */

public class ClientRequestExecutorPool implements SocketStoreFactory {

    private final KeyedResourcePool<SocketDestination, ClientRequestExecutor> pool;
    private final ClientRequestExecutorFactory factory;
    private final ClientSocketStats stats;

    private final ExecutorService executorQueuedCheckout;
    /*
     * TODO: The following thread safe data structure would ideally use a Deque
     * instead of a Queue so that concurrent put&get operations can be safely
     * done to the head. Or, maybe there is a way of refactoring the
     * processQueue method to peek the head and then safely pop the head iff the
     * checkout was successful. Or, there is a design option described below of
     * limiting ourselves to one thread per queue head which would allow us to
     * peek then pop safely.
     */
    private final ConcurrentMap<SocketDestination, Queue<AsyncRequestContext<?>>> enqueuedCheckouts;
    private final long connectionTimeoutNs;
    protected final AtomicBoolean isClosed;

    private final Logger logger = Logger.getLogger(SocketStore.class);

    public ClientRequestExecutorPool(int selectors,
                                     int maxConnectionsPerNode,
                                     int connectionTimeoutMs,
                                     int soTimeoutMs,
                                     int socketBufferSize,
                                     boolean socketKeepAlive,
                                     boolean enableJmx) {
        ResourcePoolConfig config = new ResourcePoolConfig().setIsFair(true)
                                                            .setMaxPoolSize(maxConnectionsPerNode)
                                                            .setMaxInvalidAttempts(maxConnectionsPerNode)
                                                            .setTimeout(connectionTimeoutMs,
                                                                        TimeUnit.MILLISECONDS);
        if(enableJmx) {
            stats = new ClientSocketStats();
            JmxUtils.registerMbean(new ClientSocketStatsJmx(stats),
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                             "aggregated"));
        } else {
            stats = null;
        }
        this.factory = new ClientRequestExecutorFactory(selectors,
                                                        connectionTimeoutMs,
                                                        soTimeoutMs,
                                                        socketBufferSize,
                                                        socketKeepAlive,
                                                        stats);
        this.pool = new KeyedResourcePool<SocketDestination, ClientRequestExecutor>(factory, config);
        if(stats != null) {
            this.stats.setPool(pool);
        }

        /*
         * TODO: Consider subclassing all asynchronous data members and methods
         * so that ClientReuqestExeuctorPool only has the synchronous operations
         * in it? This would allow the thread(pool) for nonblocking calls to
         * only be created if async calls are actually exposed.
         */
        this.enqueuedCheckouts = new ConcurrentHashMap<SocketDestination, Queue<AsyncRequestContext<?>>>();
        /*
         * TODO: Currently, each worker thread processes all enqueued contexts.
         * Currently, a thread pool of size 1 is created for this purpose.
         * Testing of larger thread pools is necessary to confirm correctness
         * under concurrency. Instead of a thread pool of some size greater than
         * one, we could consider one thread per destination. I.e., a single
         * thread is responsible for popping off the head of the queue while
         * many threads may enqueue at the tail. Such a design may permit us to
         * maintain FIFO ordering in the processQueue method.
         */
        this.executorQueuedCheckout = Executors.newFixedThreadPool(1);
        this.connectionTimeoutNs = TimeUnit.MILLISECONDS.toNanos(connectionTimeoutMs);
        this.executorQueuedCheckout.execute(new QueuedCheckouts());
        this.isClosed = new AtomicBoolean(false);
    }

    public ClientRequestExecutorPool(int selectors,
                                     int maxConnectionsPerNode,
                                     int connectionTimeoutMs,
                                     int soTimeoutMs,
                                     int socketBufferSize,
                                     boolean socketKeepAlive) {
        // JMX bean is disabled by default
        this(selectors,
             maxConnectionsPerNode,
             connectionTimeoutMs,
             soTimeoutMs,
             socketBufferSize,
             socketKeepAlive,
             false);
    }

    public ClientRequestExecutorPool(int maxConnectionsPerNode,
                                     int connectionTimeoutMs,
                                     int soTimeoutMs,
                                     int socketBufferSize) {
        // maintain backward compatibility of API
        this(2, maxConnectionsPerNode, connectionTimeoutMs, soTimeoutMs, socketBufferSize, false);
    }

    public ClientRequestExecutorFactory getFactory() {
        return factory;
    }

    public SocketStore create(String storeName,
                              String hostName,
                              int port,
                              RequestFormatType requestFormatType,
                              RequestRoutingType requestRoutingType) {
        SocketDestination dest = new SocketDestination(Utils.notNull(hostName),
                                                       port,
                                                       requestFormatType);
        return new SocketStore(Utils.notNull(storeName),
                               factory.getTimeout(),
                               dest,
                               this,
                               requestRoutingType);
    }

    /**
     * Checkout a socket from the pool. May return null since this call is
     * non-blocking.
     * 
     * @param destination The socket destination you want to connect to or null.
     * @return The socket
     */
    private ClientRequestExecutor nonblockingCheckout(SocketDestination destination) {
        long start = System.nanoTime();
        ClientRequestExecutor clientRequestExecutor;
        try {
            clientRequestExecutor = pool.nonblockingCheckout(destination);
        } catch(Exception e) {
            throw new UnreachableStoreException("Failure while checking out socket for "
                                                + destination + ": ", e);
        } finally {
            long end = System.nanoTime();
            if(stats != null) {
                stats.recordCheckoutTimeUs(destination, (end - start) / Time.NS_PER_US);
            }
        }
        return clientRequestExecutor;
    }

    /**
     * Checkout a socket from the pool. Will block until checkout succeeds or
     * timeout expires.
     * 
     * @param destination The socket destination you want to connect to
     * @return The socket
     */
    public ClientRequestExecutor checkout(SocketDestination destination) {
        long start = System.nanoTime();
        ClientRequestExecutor clientRequestExecutor;
        try {
            clientRequestExecutor = pool.checkout(destination);
        } catch(Exception e) {
            throw new UnreachableStoreException("Failure while checking out socket for "
                                                + destination + ": ", e);
        } finally {
            long end = System.nanoTime();
            if(stats != null) {
                stats.recordCheckoutTimeUs(destination, (end - start) / Time.NS_PER_US);
            }
        }
        return clientRequestExecutor;
    }

    /**
     * Check the socket back into the pool.
     * 
     * @param destination The socket destination of the socket
     * @param clientRequestExecutor The request executor wrapper
     */
    public void checkin(SocketDestination destination, ClientRequestExecutor clientRequestExecutor) {
        try {
            pool.checkin(destination, clientRequestExecutor);
        } catch(Exception e) {
            throw new VoldemortException("Failure while checking in socket for " + destination
                                         + ": ", e);
        }
    }

    public void close(SocketDestination destination) {
        factory.setLastClosedTimestamp(destination);
        /*
         * TODO: Determine if we need to cancel everything in async map of
         * queues in graceful manner. I think the factory and pool operations on
         * the destination will shut everything down, but am not positive.
         */
        pool.close(destination);
    }

    /**
     * Close the socket pool
     */
    public void close() {
        // unregister MBeans
        if(stats != null) {
            try {
                JmxUtils.unregisterMbean(JmxUtils.createObjectName(JmxUtils.getPackageName(ClientRequestExecutor.class),
                                                                   "aggregated"));
            } catch(Exception e) {}
            stats.close();
        }
        factory.close();
        this.isClosed.set(true);
        /*
         * TODO: Determine if we need to cancel everything in async map of
         * queues in graceful manner. I think the factory and pool close will
         * shut everything down, but am not positive.
         */
        pool.close();
    }

    public ClientSocketStats getStats() {
        return stats;
    }

    /**
     * Context necessary to setup async request for SocketDestination
     * destination and tracking details of this request.
     */
    private static class AsyncRequestContext<T> {

        public final ClientRequest<T> delegate;
        public final NonblockingStoreCallback callback;
        public final long timeoutMs;
        public final String operationName;
        private final long startTimeNs;
        private ClientRequestExecutor clientRequestExecutor;
        private boolean calledBack;

        public AsyncRequestContext(ClientRequest<T> delegate,
                                   NonblockingStoreCallback callback,
                                   long timeoutMs,
                                   String operationName) {
            this.delegate = delegate;
            this.callback = callback;
            this.timeoutMs = timeoutMs;
            this.operationName = operationName;

            this.startTimeNs = System.nanoTime();

            this.calledBack = false;
            this.clientRequestExecutor = null;
        }

    }

    public <T> void submitAsync(SocketDestination destination,
                                ClientRequest<T> delegate,
                                NonblockingStoreCallback callback,
                                long timeoutMs,
                                String operationName) {

        if(!enqueuedCheckouts.containsKey(destination)) {
            enqueuedCheckouts.putIfAbsent(destination,
                                          new ConcurrentLinkedQueue<AsyncRequestContext<?>>());
        }
        enqueuedCheckouts.get(destination).add(new AsyncRequestContext<T>(delegate,
                                                                          callback,
                                                                          timeoutMs,
                                                                          operationName));
        return;
    }

    /**
     * A worker thread that processes enqueued non-blocking checkout requests.
     * To "process" means to checkout a connection and submit the request, or to
     * directly invoke a callback upon timeout or failure.
     */
    private class QueuedCheckouts implements Runnable {

        public QueuedCheckouts() {}

        /**
         * Actually submit the enqueued async request with the checked out
         * destination.
         */
        private <T> void submitAsyncRequest(SocketDestination destination,
                                            AsyncRequestContext<T> context) {
            NonblockingStoreCallbackClientRequest<T> clientRequest = new NonblockingStoreCallbackClientRequest<T>(destination,
                                                                                                                  context.delegate,
                                                                                                                  context.clientRequestExecutor,
                                                                                                                  context.callback);
            context.clientRequestExecutor.addClientRequest(clientRequest, context.timeoutMs);
        }

        /**
         * Does a non-blocking attempt to checkout destination.
         * 
         * @param destination Socket to try and checkout.
         * @param context Context of the async request.
         * @return true iff destination is checked out. Will set
         *         context.calledback to true if callback is invoked because
         *         store is unreachable.
         */
        private <T> boolean attemptCheckout(SocketDestination destination,
                                            AsyncRequestContext<T> context) {
            ClientRequestExecutor clientRequestExecutor = null;

            try {
                clientRequestExecutor = nonblockingCheckout(destination);
                // TODO: Add back the logging Peter Bailis added
            } catch(Exception e) {
                if(!(e instanceof UnreachableStoreException))
                    e = new UnreachableStoreException("Failure in " + context.operationName + ": "
                                                      + e.getMessage(), e);
                try {
                    context.callback.requestComplete(e,
                                                     TimeUnit.NANOSECONDS.toMillis(context.startTimeNs
                                                                                   - System.nanoTime()));
                    context.calledBack = true;
                } catch(Exception ex) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(ex, ex);
                }
            }
            if(clientRequestExecutor != null) {
                context.clientRequestExecutor = clientRequestExecutor;
                return true;
            }
            return false;
        }

        /**
         * Validates that the enqueued async request is still valid. I.e., that
         * callback has not already been invoked and that timeout has not
         * expired.
         * 
         * @param destination Socket to try and checkout.
         * @param context Context of the async request.
         * @return true iff context still needs to checkout the destination.
         */
        private <T> boolean validateContext(SocketDestination destination,
                                            AsyncRequestContext<T> context) {
            if(context.calledBack) {
                return false;
            }
            if(System.nanoTime() - context.startTimeNs > connectionTimeoutNs) {
                /*
                 * TODO: The following exception "stack" mimics what would have
                 * happened in the blocking "asynchronous" connection checkout
                 * code path. Should refactor / cleanup / review exception
                 * patterns for both synchronous and asynchronous connection
                 * checkout code paths.
                 */
                Exception eTO = new TimeoutException("Could not acquire resource in "
                                                     + (connectionTimeoutNs / Time.NS_PER_MS)
                                                     + " ms.");
                Exception eUS1 = new UnreachableStoreException("Failure while checking out socket for "
                                                                       + destination
                                                                       + ": "
                                                                       + eTO.getMessage(),
                                                               eTO);
                Exception eUS2 = new UnreachableStoreException("Failure in "
                                                               + context.operationName + ": "
                                                               + eUS1.getMessage(), eUS1);
                context.callback.requestComplete(eUS2,
                                                 TimeUnit.NANOSECONDS.toMillis(context.startTimeNs
                                                                               - System.nanoTime()));
                context.calledBack = true;
                return false;
            }

            return true;
        }

        /*
         * TODO: Review usage of templates & generics for the
         * AsyncRequestContext type. Ideally, the method processQueue would
         * actually use the template parameter T.
         */
        /**
         * Processes enqueued async requests for the specified destination. If
         * there are enqueued requests, at least one such request will be
         * "processed". I.e., at least its timeout will be checked.
         * 
         * @param destination Specific Socket to try and checkout.
         * @param checkoutQueue Queue of async requests for specified
         *        destination
         * @return true means context was processed (either by submitting
         *         request or timing out). false means queue is empty or a
         *         connection could not be checked out for this destination.
         */
        private <T> boolean processQueue(SocketDestination destination,
                                         Queue<AsyncRequestContext<?>> checkoutQueue) {
            AsyncRequestContext<?> context = checkoutQueue.poll();
            if(context == null) {
                /*
                 * TODO: destinations are never removed from the
                 * enqueuedCheckouts map. I.e., once a destination is added, it
                 * is forever in the map, potentially with a null queue. If
                 * there is lots of SocketDestination churn, then eventually,
                 * this would consume a lot of useless memory. Would need to
                 * remove the SocketDestination (and corresponding null queue)
                 * in a thread safe manner.
                 */
                return false;
            }
            if(!validateContext(destination, context)) {
                return true;
            }
            if(attemptCheckout(destination, context)) {
                submitAsyncRequest(destination, context);
                return true;
            }
            // Add context back to end of queue. This breaks FIFO processing.
            // TODO: Consider switching to ConcurrentDeque in Java 1.7
            checkoutQueue.add(context);
            return false;
        }

        public void run() {
            /*
             * TODO: What is the Java best practice for implementing a loop such
             * as this? This loop runs continuously even when there is no work
             * to be done. Not sure if the "yield" is enough to keep this thread
             * from interfering with other work.
             * 
             * A cleaner approach may be to notify the thread upon submitAsync
             * and upon checkin of a connection. We would also need to
             * periodically awake and process timeouts. In that case, I am not
             * sure if the right thing to do is to awake at the next scheduled
             * timeout, or periodically (just in case we forget to notify upon
             * some event). With periodic awakes, may want to have some sort of
             * backoff so that we are not constantly waking up when no contexts
             * are enqueued.
             */
            while(!isClosed.get()) {
                for(SocketDestination destination: enqueuedCheckouts.keySet()) {
                    Queue<AsyncRequestContext<?>> checkoutQueue = enqueuedCheckouts.get(destination);
                    if(checkoutQueue != null) {
                        while(processQueue(destination, checkoutQueue)) {}
                    }
                    Thread.yield();
                }
            }
        }

        private class NonblockingStoreCallbackClientRequest<T> implements ClientRequest<T> {

            private final SocketDestination destination;
            private final ClientRequest<T> clientRequest;
            private final ClientRequestExecutor clientRequestExecutor;
            private final NonblockingStoreCallback callback;
            private final long startNs;

            private volatile boolean isComplete;

            public NonblockingStoreCallbackClientRequest(SocketDestination destination,
                                                         ClientRequest<T> clientRequest,
                                                         ClientRequestExecutor clientRequestExecutor,
                                                         NonblockingStoreCallback callback) {
                this.destination = destination;
                this.clientRequest = clientRequest;
                this.clientRequestExecutor = clientRequestExecutor;
                this.callback = callback;

                this.startNs = System.nanoTime();
            }

            private void invokeCallback(Object o, long requestTime) {
                if(callback != null) {
                    try {
                        if(logger.isDebugEnabled()) {
                            logger.debug("Async request end; requestRef: "
                                         + System.identityHashCode(clientRequest)
                                         + " time: "
                                         + System.currentTimeMillis()
                                         + " server: "
                                         + clientRequestExecutor.getSocketChannel()
                                                                .socket()
                                                                .getRemoteSocketAddress()
                                         + " local socket: "
                                         + clientRequestExecutor.getSocketChannel()
                                                                .socket()
                                                                .getLocalAddress()
                                         + ":"
                                         + clientRequestExecutor.getSocketChannel()
                                                                .socket()
                                                                .getLocalPort() + " result: " + o);
                        }

                        callback.requestComplete(o, requestTime);
                    } catch(Exception e) {
                        if(logger.isEnabledFor(Level.WARN))
                            logger.warn(e, e);
                    }
                }
            }

            public void complete() {
                try {
                    clientRequest.complete();
                    Object result = clientRequest.getResult();

                    invokeCallback(result, (System.nanoTime() - startNs) / Time.NS_PER_MS);
                } catch(Exception e) {
                    invokeCallback(e, (System.nanoTime() - startNs) / Time.NS_PER_MS);
                } finally {
                    checkin(destination, clientRequestExecutor);
                    isComplete = true;
                }
            }

            public boolean isComplete() {
                return isComplete;
            }

            public boolean formatRequest(DataOutputStream outputStream) {
                return clientRequest.formatRequest(outputStream);
            }

            public T getResult() throws VoldemortException, IOException {
                return clientRequest.getResult();
            }

            public boolean isCompleteResponse(ByteBuffer buffer) {
                return clientRequest.isCompleteResponse(buffer);
            }

            public void parseResponse(DataInputStream inputStream) {
                clientRequest.parseResponse(inputStream);
            }

            public void timeOut() {
                clientRequest.timeOut();
                invokeCallback(new StoreTimeoutException("ClientRequestExecutor timed out. Cannot complete request."),
                               (System.nanoTime() - startNs) / Time.NS_PER_MS);
                checkin(destination, clientRequestExecutor);
            }

            public boolean isTimedOut() {
                return clientRequest.isTimedOut();
            }
        }
    }
}
