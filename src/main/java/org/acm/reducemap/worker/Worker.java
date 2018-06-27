package org.acm.reducemap.worker;

import org.acm.reducemap.common.RPCConfig;
import org.acm.reducemap.master.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayDeque;
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

    class WorkItem {
        int workId;
        int workType;
        String param;
        WorkItem(int wid, int wt, String p){
            workId = wid;
            workType = wt;
            param = p;
        }
    }

    private boolean isRunning;
    private int workerId;
    private String localAddress;
    private int localPort;
    private HashMap<Integer,String> workDescMap = new HashMap<>();
    private ArrayDeque<WorkItem> workQueue = new ArrayDeque<>();

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
        String src = workDescMap.get(workType);
        if (src == null) {
            logger.warning("Work description not exist");
            reply.setStatus(-1);
            return;
        }
        workQueue.add(new WorkItem(workId,workType,req.getParamHandle()));
        reply.setStatus(0);
    }

    void work() {
        WorkItem item = workQueue.pop();
        if (item == null) return;
        int workId = item.workId;
        int workType = item.workType;
        String param = item.param;
        System.out.println("do Work:"+workId);
        String desc = "./output_"+workerId+"_"+workId+"_"+workType+".out";
        int ret = exec.run(desc,workDescMap.get(workType),param);
//        int ret = 0;
        System.out.println("done:"+workId+" ret:"+ret);
        JobCompleteReply rep = (JobCompleteReply)
                client.call("jobComplete",JobCompleteRequest.newBuilder()
                .setWorkerId(workerId).setJobId(workId).setJobType(workType).setResultHandle(desc).setRet(ret).build());
        System.out.println("get JobComplete status:"+rep.getStatus());
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
        System.setProperty("python.import.site","false");
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
