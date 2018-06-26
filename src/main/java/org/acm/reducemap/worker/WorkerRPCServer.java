package org.acm.reducemap.worker;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.logging.Logger;

class WorkerRPCServer {
    private static Logger logger;
    private int port;

    private Server server;
    private static Worker worker;

    WorkerRPCServer(Worker wk, Logger log, int p) {
        worker = wk;
        logger = log;
        port = p;
    }

    void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new WorkerImpl())
                .build()
                .start();
        logger.info("ReduceMap Worker RPCServer started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            WorkerRPCServer.this.stop();
            System.err.println("*** server shut down");
        }));
    }

    void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    static class WorkerImpl extends WorkerGrpc.WorkerImplBase {

        @Override
        public void assignWork(AssignWorkRequest req, StreamObserver<AssignWorkReply> responseObserver) {
            AssignWorkReply.Builder reply = AssignWorkReply.newBuilder();
            worker.onAssignWork(reply,req);
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        }

        @Override
        public void haltWorker(HaltWorkerRequest request, StreamObserver<HaltWorkerReply> responseObserver) {
            HaltWorkerReply.Builder reply = HaltWorkerReply.newBuilder();
            worker.onHaltWorker(reply,request);
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        }

        @Override
        public void descWork(DescWorkRequest request, StreamObserver<DescWorkReply> responseObserver) {
            DescWorkReply.Builder reply = DescWorkReply.newBuilder();
            worker.onDescWork(reply,request);
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        }
    }
}
