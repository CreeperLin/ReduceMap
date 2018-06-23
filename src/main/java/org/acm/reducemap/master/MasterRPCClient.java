package org.acm.reducemap.master;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.acm.reducemap.worker.*;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MasterRPCClient {
    private static final Logger logger = Logger.getLogger(MasterRPCClient.class.getName());

    private final ManagedChannel channel;
    private final WorkerGrpc.WorkerBlockingStub blockingStub;
    private final WorkerGrpc.WorkerStub stub;

    /** Construct client connecting to worker at {@code host:port}. */
    MasterRPCClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // disable TLS to avoid needing certificates.
                .usePlaintext()
                .build());
    }

    private MasterRPCClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = WorkerGrpc.newBlockingStub(channel);
        stub = WorkerGrpc.newStub(channel);
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
            e.printStackTrace();
        }
        return null;
    }

    boolean asyncCall(String methodName, Object req, StreamObserver<?> observer) {
        try {
            stub.getClass().getMethod(methodName, req.getClass(), StreamObserver.class).invoke(stub, req, observer);
            return true;
        } catch (StatusRuntimeException e) {
            observer.onError(e);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

}
