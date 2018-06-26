package org.acm.reducemap.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.acm.reducemap.master.MasterGrpc;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

class ClientRPCClient {

    private static final Logger logger = Logger.getLogger(ClientRPCClient.class.getName());

    private final ManagedChannel channel;
    private final MasterGrpc.MasterBlockingStub blockingStub;
    private final MasterGrpc.MasterStub stub;

    /** Construct client connecting to master at {@code host:port}. */
    ClientRPCClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // disable TLS to avoid needing certificates.
                .usePlaintext()
                .build());
    }

    private ClientRPCClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = MasterGrpc.newBlockingStub(channel);
        stub = MasterGrpc.newStub(channel);
    }

    void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    // provide general call method
    Object call(String methodName, Object req) {
        try {
            return blockingStub.getClass().getMethod(methodName, req.getClass()).invoke(blockingStub, req);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        } catch (Exception e) {
            logger.log(Level.WARNING, "Invalid RPC Call: {0}", e.toString());
        }
        return null;
    }

    boolean asyncCall(String methodName, Object req, StreamObserver observer) {
        try {
            stub.getClass().getMethod(methodName, req.getClass(), StreamObserver.class).invoke(stub, req, observer);
            return true;
        } catch (StatusRuntimeException e) {
            observer.onError(e);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Invalid RPC Call: {0}", methodName);
        }
        return false;
    }
}
