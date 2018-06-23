package org.acm.reducemap.master;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.acm.reducemap.worker.*;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MasterRPCClient {
    private static final Logger logger = Logger.getLogger(MasterRPCClient.class.getName());

    private final ManagedChannel channel;
    private final WorkerGrpc.WorkerBlockingStub blockingStub;

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

    //provide interface for master RPC call (cannot be generated currently)
    HaltWorkerReply haltWorker(int reason) {
        logger.info("Will try to halt worker");
        HaltWorkerRequest req = HaltWorkerRequest.newBuilder().setReason(reason).build();
        HaltWorkerReply reply;
        try {
            reply = blockingStub.haltWorker(req);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return null;
        }
        return reply;
    }

    AssignWorkReply assignWork(int workType, int workId) {
        logger.info("Will try to request AssignWork: type:"+workType+" id:"+workId);
        AssignWorkRequest request = AssignWorkRequest.newBuilder().setWorkType(workType).setWorkId(workId).build();
        AssignWorkReply reply;
        try {
            reply = blockingStub.assignWork(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return null;
        }
        return reply;
    }
}
