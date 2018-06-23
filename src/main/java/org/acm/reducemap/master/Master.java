package org.acm.reducemap.master;

import org.acm.reducemap.common.RPCAddress;
import org.acm.reducemap.common.RPCConfig;

import java.io.IOException;
import java.util.logging.Logger;

public class Master {

    private static final Logger logger = Logger.getLogger(Master.class.getName());
    private MasterRPCServer server;
    WorkerManager workerMan = new WorkerManager();
    private JobScheduler jobScheduler = new JobScheduler(this,logger);

    private Master(int port) {
        server = new MasterRPCServer(this,logger,port);
    }

    //implementation begins here
    // on receiving worker RPC calls
    void onRegister(RegisterReply.Builder reply, RegisterRequest req) {
        logger.info("recvReq Register ip:"+req.getIpAddress()+" port:"+req.getPort());
        int id = workerMan.registerWorker(new RPCAddress(req.getIpAddress(),req.getPort()));
        reply.setWorkerId(id);
    }

    void onHeartbeat(HeartbeatReply.Builder reply, HeartbeatRequest req) {
        logger.info("recvReq Heartbeat id:"+req.getWorkerId());
        workerMan.keepAliveWorker(req.getWorkerId());
        reply.setStatus(1);
    }

    // on Completing all tasks
    private void onComplete() {
        logger.info("All task complete, stopping");
        stop();
    }

    private void run() throws IOException, InterruptedException {
        server.start();
        jobScheduler.schedule();
        onComplete();
        server.blockUntilShutdown();
    }

    private void stop() {
        workerMan.haltAllWorker();
        server.stop();
    }

    //usage: [port]
    public static void main(String[] args) throws IOException, InterruptedException {
        int port;
        if (args.length>0) {
            port = Integer.parseInt(args[0]);
        } else {
            port = RPCConfig.masterPort;
        }
        Master master = new Master(port);
        master.run();
    }
}
