package org.acm.reducemap.master;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.acm.reducemap.common.RPCConfig;
import org.acm.reducemap.worker.AssignWorkReply;
import org.acm.reducemap.worker.AssignWorkRequest;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

class JobScheduler {

    private Master master;
    private Logger logger;
    private ArrayDeque<JobType> jobQueue = new ArrayDeque<>();
    private HashMap<Integer,Boolean> compMap = new HashMap<>();

    private int result = 0; //demo

    public class JobType {
        private int SerialNum;
        private String para;
        private int WorkType;
        JobType(int wt, int x, String p){
            WorkType = wt;
            SerialNum = x;
            para = p;
        }
        int getNum(){
            return SerialNum;
        }
    }
    JobScheduler(Master mas, Logger log) {
        master = mas;
        logger = log;
    }

    private void onJobComplete(int workerId, int jobId, AssignWorkReply reply) {
        int status = reply.getStatus();
        System.out.println("get AssignWork reply id:"+workerId+" status:"+status);
        result += status;
        master.workerMan.freeWorker(workerId);
        compMap.put(jobId,true);
        if (isAllComplete()) {
            System.out.println("complete:"+result); //338350
            master.onComplete();
        }
    }

    private boolean isAllComplete() {
        return compMap.size() == 100;
    }

    synchronized void addJob(JobType job) {
        jobQueue.add(job);
    }

    private synchronized JobType getJob() {
        if (jobQueue.isEmpty()) return null;
        return jobQueue.pop();
    }

    void schedule() throws InterruptedException{
//        int t = 100;
//        for (int i = 1;i<=t;++i) jobQueue.add(i);

        JobType job;
        while ((job = getJob())!= null){
            int jobId = job.SerialNum;
            logger.info("Schedule:AssignWork:"+job.SerialNum);
            Vector<WorkerManager.workerInfo> avail = new Vector<>();
            while (true) {
                while (true){
                    master.workerMan.getAvailableWorkers(avail);
                    if (!avail.isEmpty()) break;
                    System.out.println("no worker available, retrying...");
                    Thread.sleep(RPCConfig.masterScheduleRetryInterval);
                }
                int idx = new Random().nextInt(avail.size());
                WorkerManager.workerInfo info = avail.get(idx);
                int workerId = info.workerId;
                System.out.println("assigning work to:"+workerId);
                MasterRPCClient rpc = info.cli; // or rpc = workerMan.getWorkerRPC(workerId)

                boolean succ;
                // sync call demo
//                AssignWorkReply reply = (AssignWorkReply)
//                        rpc.call("assignWork",
//                                AssignWorkRequest.newBuilder().setWorkId(i).setWorkType(i%5).build());
//                succ = (reply != null);

                // async call demo
                int finalJobId = jobId;
                StreamObserver<AssignWorkReply> replyObserver = new StreamObserver<AssignWorkReply>() {
                    int wid = workerId;
                    int jid = finalJobId;

                    @Override
                    public void onNext(AssignWorkReply value) {
                        onJobComplete(wid, jid, value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.log(Level.WARNING,"Assign work Failed: {0}", ((StatusRuntimeException)t).getStatus());
                    }

                    @Override
                    public void onCompleted() {}
                };
                succ = rpc.asyncCall("assignWork",
                        AssignWorkRequest.newBuilder().setWorkId(jobId).setWorkType(job.WorkType).setParamHandle(job.para).build(),replyObserver);

                if (!succ) { //not working in async call?
                    logger.warning("worker down, retrying");
                    Thread.sleep(RPCConfig.masterScheduleRetryInterval);
                    continue;
                }
                System.out.println("assigned: worker:"+workerId+" job:"+jobId);
                master.workerMan.busyWorker(workerId, job);
                break;
            }
        }
        logger.info("Schedule:complete");
    }

}
