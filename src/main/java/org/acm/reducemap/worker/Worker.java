package org.acm.reducemap.worker;

import org.acm.reducemap.common.RPCAddress;
import org.acm.reducemap.common.RPCConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.util.logging.Logger;

public class Worker {

    int localPort;
    private WorkerRPCServer server;
    private WorkerRPCClient client;
    private static final Logger logger = Logger.getLogger(Worker.class.getName());

    private int curStatus = 0; // demo

    private Worker(int port, String masterAddr, int masterPort) {
        localPort = port;
        server = new WorkerRPCServer(this,logger,port);
        client = new WorkerRPCClient(masterAddr,masterPort);
    }

    //implementation begins here
    void onAssignWork(AssignWorkReply.Builder reply, AssignWorkRequest req) {
        logger.info("recvReq AssignWork: workId:"+req.getWorkId()+" workType:"+req.getWorkType());
        reply.setStatus(++curStatus);
    }

    void onHaltWorker(HaltWorkerReply.Builder reply, HaltWorkerRequest req) {
        logger.info("recvReq Halt");
        reply.setStatus(0);
        server.stop();
    }

    private void run() throws IOException, InterruptedException {
        String t = InetAddress.getLocalHost().getHostAddress();
//        String t = RPCConfig.getLocalIpAddr();
        logger.info("worker started localhost:"+t);
        server.start();

        client.register(t,localPort);

        server.blockUntilShutdown();
        client.shutdown();
    }

    //usage: [port] master_address master_port
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length<2) {
            logger.warning("specify master address and port");
            return;
        }
        int port;
        if (args.length>2) {
            port = Integer.parseInt(args[2]);
        } else {
            port = RPCConfig.workerPort;
        }
        String masterAddr = args[0];
        int masterPort = Integer.parseInt(args[1]);
        System.out.println("master addr:"+masterAddr+" port:"+masterPort);
        Worker worker = new Worker(port,masterAddr,masterPort);
        worker.run();
    }
}
