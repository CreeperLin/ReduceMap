package org.acm.reducemap.master;

import org.acm.reducemap.common.RPCAddress;
import org.acm.reducemap.common.RPCConfig;
import org.acm.reducemap.worker.AssignWorkReply;

import java.io.IOException;
import java.util.Random;
import java.util.Vector;
import java.util.logging.Logger;

public class Master {

    private static final Logger logger = Logger.getLogger(Master.class.getName());
    private MasterRPCServer server;
    private WorkerManager workerMan = new WorkerManager();

    private int curWorkerId = 0; //demo

    private Master(int port) {
        server = new MasterRPCServer(this,logger,port);
    }

    //demo
    synchronized private int getCurWorkerId() {
        return ++curWorkerId;
    }

    //implementation begins here
    // on receiving worker RPC calls
    void onRegister(RegisterReply.Builder reply, RegisterRequest req) {
        logger.info("recvReq Register ip:"+req.getIpAddress()+" port:"+req.getPort());
        int id = getCurWorkerId();
        workerMan.registerWorker(new RPCAddress(req.getIpAddress(),req.getPort()), id);
        reply.setWorkerId(id);
    }

    void onHeartbeat(HeartbeatReply.Builder reply, HeartbeatRequest req) {
        logger.info("recvReq Heartbeat id:"+req.getWorkerId());
        workerMan.keepAliveWorker(req.getWorkerId());
        reply.setStatus(1);
    }

    private void testSchedule() {
        int t = 100;
        for (int i=1;i<=t;++i) {
            logger.info("Schedule:AssignWork:"+i);
            Vector<WorkerManager.workerInfo> avail = new Vector<>();
            while (true) {
                while (avail.isEmpty()){
                    workerMan.getAliveWorkers(avail);
                }
                int idx = new Random().nextInt(avail.size());
                WorkerManager.workerInfo info = avail.get(idx);
//              System.out.println("info:"+info.workerId);
                MasterRPCClient rpc = info.cli; // or rpc = workerMan.getWorkerRPC(info.workerId)
                AssignWorkReply reply = rpc.assignWork(i%5,i); //demo
                if (reply==null) continue;
                int status = reply.getStatus();
                System.out.println("get worker status:"+status);
                break;
            }
        }
        logger.info("Schedule:complete");
    }

    private void run() throws IOException, InterruptedException {
        server.start();
        testSchedule();
        workerMan.haltAllWorker();
        server.stop();
//        server.blockUntilShutdown();
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
