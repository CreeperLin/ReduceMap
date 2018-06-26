package org.acm.reducemap.worker;

import org.acm.reducemap.common.RPCConfig;
import org.acm.reducemap.master.HeartbeatReply;
import org.acm.reducemap.master.HeartbeatRequest;
import org.acm.reducemap.master.RegisterReply;
import org.acm.reducemap.master.RegisterRequest;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.logging.Logger;

public class Worker {
    // send heartbeat
    class HeartbeatThread implements Runnable {
        @Override
        public void run() {
            HeartbeatReply reply;
            while (isRunning) {
                try {
                    Thread.sleep(RPCConfig.workerHeartbeatInterval);
                } catch (InterruptedException ignored) {}
                System.out.println("sending heartbeat");
                reply = (HeartbeatReply)
                        client.call("heartbeat",HeartbeatRequest.newBuilder().
                                setWorkerId(workerId).
                                setIpAddress(localAddress).
                                setStatus(1).build());
                if (reply!=null) continue;
                logger.warning("heartbeat: master down, retrying...");
                try {
                    Thread.sleep(RPCConfig.workerHeartbeatRetryInterval);
                } catch (InterruptedException ignored) {}
            }
        }
    }

    private boolean isRunning;
    private int workerId;
    private String localAddress;
    private int localPort;
    private HashMap<Integer,String> workDescMap = new HashMap<>();

    private WorkerRPCServer server;
    private WorkerRPCClient client;
    private JyExecutor exec;
    private static final Logger logger = Logger.getLogger(Worker.class.getName());

    private Worker(int port, String masterAddr, int masterPort) {
        localPort = port;
        server = new WorkerRPCServer(this,logger,port);
        client = new WorkerRPCClient(masterAddr,masterPort);
        exec = new JyExecutor();
    }

    //implementation begins here
    //on receiving RPC calls
    void onAssignWork(AssignWorkReply.Builder reply, AssignWorkRequest req) {
        int workId = req.getWorkId(), workType = req.getWorkType();
        logger.info("recvReq AssignWork: workId:"+workId+" workType:"+workType);
        System.out.println("do Work:"+workId);
        String src = workDescMap.get(workId);
        if (src == null) {
            logger.warning("Work description not exist");
            reply.setStatus(-1);
            return;
        }
        int ret = exec.run(src,req.getParamHandle());
        System.out.println("done:"+workId+" ret:"+ret);
        reply.setStatus(ret);
    }

    void onHaltWorker(HaltWorkerReply.Builder reply, HaltWorkerRequest req) {
        logger.info("recvReq Halt:"+req.getReason());
        reply.setStatus(0);
        server.stop();
        isRunning = false;
    }

    void onDescWork(DescWorkReply.Builder reply, DescWorkRequest req) {
        logger.info("recvReq DescWork:"+req.getWorkType());
        workDescMap.put(req.getWorkType(),req.getExec());
        reply.setStatus(0);
    }

    //on RPC call reply
    private void onReplyRegister(RegisterReply reply) {
        logger.info("get WorkerId: " + reply.getWorkerId());
        workerId = reply.getWorkerId();
    }

    private void run() throws IOException, InterruptedException {
        localAddress = InetAddress.getLocalHost().getHostAddress()+":"+localPort;
//        String localAddress = RPCConfig.getLocalIpAddr(); // real local ip address
        logger.info("worker started localhost:"+localAddress);
        server.start();
        isRunning = true;

        RegisterReply reply;
        while (true) {
            reply = (RegisterReply) client.call("register",RegisterRequest.newBuilder().setIpAddress(localAddress).build());
            if (reply!=null) break;
            logger.warning("master down, reconnecting...");
            try {
                Thread.sleep(RPCConfig.workerRegisterRetryInterval);
            } catch (InterruptedException ignored) {}
        }
        onReplyRegister(reply);

        Thread hbThread = new Thread(new HeartbeatThread());

        hbThread.start();

        server.blockUntilShutdown();
        client.shutdown();
    }

    //usage: master_address master_port [port]
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
