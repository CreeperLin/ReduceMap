package org.acm.reducemap.worker;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.acm.reducemap.master.MasterGrpc;
import org.acm.reducemap.master.RegisterReply;
import org.acm.reducemap.master.RegisterRequest;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkerRPCClient {
    private static final Logger logger = Logger.getLogger(WorkerRPCClient.class.getName());

    private final ManagedChannel channel;
    private final MasterGrpc.MasterBlockingStub blockingStub;

    /** Construct client connecting to master at {@code host:port}. */
    WorkerRPCClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // disable TLS to avoid needing certificates.
                .usePlaintext()
                .build());
    }

    private WorkerRPCClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = MasterGrpc.newBlockingStub(channel);
    }

    void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    // provide general call method
    Object call(String methodName, Object req) {
        try {
            return blockingStub.getClass().getMethod(methodName, req.getClass()).invoke(blockingStub, req);
        } catch (Exception e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getMessage());
        }
        return null;
    }

    //provide interface for worker RPC call (deprecated?)
    RegisterReply register(String ip, int port) {
        logger.info("Will try to register: ip:"+ip+" port:"+port);
        RegisterRequest request = RegisterRequest.newBuilder().setIpAddress(ip).setPort(port).build();
        RegisterReply response;
        try {
            response = blockingStub.register(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return null;
        }
        return response;
    }

}
