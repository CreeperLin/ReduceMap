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

    //provide interface for master RPC call
    void haltWorker() {
        logger.info("Will try to halt");
        HaltWorkerRequest req = HaltWorkerRequest.newBuilder().setReason(1).build();
        HaltWorkerReply reply;
        try {
            reply = blockingStub.haltWorker(req);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("get status: " + reply.getStatus());
    }

    void assignWork(int workType, int workId) {
        logger.info("Will try to request AssignWork: type:"+workType+" id:"+workId);
        AssignWorkRequest request = AssignWorkRequest.newBuilder().setWorkType(workType).setWorkId(workId).build();
        AssignWorkReply response;
        try {
            response = blockingStub.assignWork(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("get status: " + response.getStatus());
    }
}
