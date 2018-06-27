package org.acm.reducemap.master;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.logging.Logger;

public class MasterRPCServer {
    private static Logger logger;

    private Server server;
    private static Master master;
    private int port;

    MasterRPCServer(Master mas, Logger log, int p) {
        master = mas;
        logger = log;
        port = p;
    }

    void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MasterImpl())
                .build()
                .start();
        logger.info("ReduceMap Master RPCServer started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MasterRPCServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
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

    static class MasterImpl extends MasterGrpc.MasterImplBase {

        @Override
        public void register(RegisterRequest req, StreamObserver<RegisterReply> responseObserver) {
            RegisterReply.Builder reply = RegisterReply.newBuilder();
            master.onRegister(reply,req);
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        }

        @Override
        public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatReply> responseObserver) {
            HeartbeatReply.Builder reply = HeartbeatReply.newBuilder();
            master.onHeartbeat(reply,request);
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        }

        @Override
        public void newWork(NewWorkRequest request, StreamObserver<NewWorkReply> responseObserver) {
            NewWorkReply.Builder reply = NewWorkReply.newBuilder();
            master.onNewWork(reply,request);
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        }

        @Override
        public void execute(ExecuteRequest request, StreamObserver<ExecuteReply> responseObserver) {
            ExecuteReply.Builder reply = ExecuteReply.newBuilder();
            master.onExecute(reply,request);
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        }

        @Override
        public void jobComplete(JobCompleteRequest request, StreamObserver<JobCompleteReply> responseObserver) {
            JobCompleteReply.Builder reply = JobCompleteReply.newBuilder();
            master.onJobComplete(reply,request);
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        }
    }
}
