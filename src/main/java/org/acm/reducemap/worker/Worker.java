package org.acm.reducemap.worker;

import org.acm.reducemap.common.RPCConfig;
import org.acm.reducemap.master.HeartbeatReply;
import org.acm.reducemap.master.HeartbeatRequest;
import org.acm.reducemap.master.RegisterReply;
import org.acm.reducemap.master.RegisterRequest;

import java.io.IOException;
import java.net.InetAddress;
import java.util.logging.Logger;

public class Worker {
    class HeartbeatThread implements Runnable {
        @Override
        public void run() {
            HeartbeatReply reply = null;
            while (isRunning) {
                int sleepTime = 10000;
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ignored) {}
                System.out.println("sending heartbeat");
                reply = (HeartbeatReply)
                        client.call("heartbeat",HeartbeatRequest.newBuilder().
                                setWorkerId(workerId).
                                setTimestamp((int)System.currentTimeMillis()/1000).build());
                if (reply!=null) continue;
                logger.warning("heartbeat: master down, retrying...");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ignored) {}
            }
        }
    }

    private boolean isRunning;
    private int workerId;
    private int localPort;
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
    //on receiving RPC calls
    void onAssignWork(AssignWorkReply.Builder reply, AssignWorkRequest req) {
        logger.info("recvReq AssignWork: workId:"+req.getWorkId()+" workType:"+req.getWorkType());
        System.out.println("do Work:"+req.getWorkId());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {}
        reply.setStatus(++curStatus);
    }

    void onHaltWorker(HaltWorkerReply.Builder reply, HaltWorkerRequest req) {
        logger.info("recvReq Halt");
        reply.setStatus(0);
        server.stop();
        isRunning = false;
    }

    //on RPC call reply
    private void onReplyRegister(RegisterReply reply) {
        logger.info("get WorkerId: " + reply.getWorkerId());
        workerId = reply.getWorkerId();
    }

    private void run() throws IOException, InterruptedException {
        String t = InetAddress.getLocalHost().getHostAddress();
//        String t = RPCConfig.getLocalIpAddr();
        logger.info("worker started localhost:"+t);
        server.start();
        isRunning = true;

        RegisterReply reply = null;
        while (true) {
            reply = (RegisterReply) client.call("register",RegisterRequest.newBuilder().setIpAddress(t).setPort(localPort).build());
            if (reply!=null) break;
            logger.warning("master down, reconnecting...");
            try {
                Thread.sleep(3000);
            } catch (InterruptedException ignored) {}
        }
        onReplyRegister(reply);

        Thread hbThread = new Thread(new HeartbeatThread());

        hbThread.start();

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
