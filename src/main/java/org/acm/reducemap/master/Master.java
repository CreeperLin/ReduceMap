package org.acm.reducemap.master;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.acm.reducemap.common.RPCConfig;
import org.acm.reducemap.worker.DescWorkReply;
import org.acm.reducemap.worker.DescWorkRequest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
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
                    System.out.println("job reschedule: worker:"+i.workerId+" jobId:"+i.curJob.getNum());
                    jobScheduler.addJob(i.curJob);
                    i.curJob = null;
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
    private int curWorkType = 0;
    private HashMap<Integer,String> workDescMap = new HashMap<>();

    private Master(int port) {
        server = new MasterRPCServer(this,logger,port);
    }

    //implementation begins here
    private int getNewWorkType() {
        return ++curWorkType;
    }

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

    void onJobComplete(JobCompleteReply.Builder reply, JobCompleteRequest req) {
        logger.info("recvReq JobComplete:"+req.getJobId());
        workerMan.freeWorker(req.getWorkerId());
        jobScheduler.onJobComplete(req);
        reply.setStatus(0);
    }

    // client calls
    void onNewWork(NewWorkReply.Builder reply, NewWorkRequest req) {
        int wt = getNewWorkType();
        String exec = req.getExecHandle();
        logger.info("recvReq NewWork:"+wt+" exec:"+exec);

        StringBuilder srcb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(exec));
            String t;
            while((t=br.readLine())!=null){
                srcb.append(t).append('\n');
            }
        } catch (Exception e) {
            e.printStackTrace();
            reply.setWorkType(-1);
            return;
        }

        workDescMap.put(wt,srcb.toString());

        Vector<WorkerManager.workerInfo> worker = workerMan.getAllWorkers();
        worker.forEach(i->{
            DescWorkReply rep = (DescWorkReply)
                    i.cli.call("descWork",DescWorkRequest.newBuilder().setWorkType(wt).setExec(srcb.toString()).build());
            System.out.println("descWork:"+rep.getStatus());
        });
        reply.setWorkType(wt);
    }

    private void queue(int wt, int lowbound, int upbound) {
        int counter = 1;
        int curl = lowbound;
        int step = (upbound - lowbound) / 10;
        for (int i = 1;i<=10;++i) {
            JsonObject json = new JsonObject();
            json.addProperty("a",curl);
            curl += step;
            json.addProperty("b",curl);
            String para = json.toString();
            JobScheduler.JobType job = jobScheduler.new JobType(wt, i, para);
            jobScheduler.addJob(job);
        }
    }

    void onExecute(ExecuteReply.Builder reply, ExecuteRequest req) {
        int wt = req.getWorkType();
        String param = req.getParamHandle();
        logger.info("recvReq Execute:"+wt+" param:"+param);
        JsonParser parser = new JsonParser();
        JsonObject jb = (JsonObject) parser.parse(param);
        Set<String> keySet = jb.keySet();
        int lowbound = jb.get("a").getAsInt();
        int upbound = jb.get("b").getAsInt();
        queue(wt,lowbound,upbound);
        try {
            jobScheduler.schedule();
        } catch (InterruptedException ignored) {}
        reply.setStatus(0);
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
