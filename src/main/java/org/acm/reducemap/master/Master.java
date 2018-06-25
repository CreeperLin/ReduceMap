package org.acm.reducemap.master;

import org.acm.reducemap.common.RPCConfig;

import java.io.IOException;
import java.util.Vector;
import java.util.logging.Logger;

public class Master {
    // background tasks
    class BackgroundActivity implements Runnable {

        @Override
        public void run() {
            while (isRunning) {
                try {
                    Thread.sleep(RPCConfig.masterBackgroundInterval);
                } catch (InterruptedException ignored) {}
                System.out.println("background activity:");
                // dead & overdue worker detection and job reschedule
                Vector<WorkerManager.workerInfo> retireWorker = workerMan.getDeadWorkers();
                retireWorker.addAll(workerMan.getOverdueWorkers());
                for (WorkerManager.workerInfo i : retireWorker) {
                    if (!i.isBusy) continue;
                    i.isBusy = false;
                    System.out.println("job reschedule: worker:"+i.workerId+" jobId:"+i.curWorkId);
                    jobScheduler.addJob(i.curWorkId);
                    i.curWorkId = 0;
                    i.lastAssigned = 0;
                }
            }
        }
    }

    private boolean isRunning;
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
        logger.info("recvReq Register ip:"+req.getIpAddress());
        int id = workerMan.registerWorker(req.getIpAddress());
        reply.setWorkerId(id);
    }

    void onHeartbeat(HeartbeatReply.Builder reply, HeartbeatRequest req) {
        logger.info("recvReq Heartbeat id:"+req.getWorkerId()+" ip:"+req.getIpAddress());
        workerMan.keepAliveWorker(req.getWorkerId(),req.getIpAddress());
        reply.setStatus(1);
    }

    // on Completing all tasks
    void onComplete() {
        logger.info("All task complete, stopping");
        stop();
    }

    private void run() throws IOException, InterruptedException {
        isRunning = true;
        server.start();
        Thread bgThread = new Thread(new BackgroundActivity());
        bgThread.start();
        jobScheduler.schedule();
        server.blockUntilShutdown();
    }

    private void stop() {
        workerMan.haltAllWorker();
        server.stop();
        isRunning = false;
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
