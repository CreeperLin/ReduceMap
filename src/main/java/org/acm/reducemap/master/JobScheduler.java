package org.acm.reducemap.master;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.acm.reducemap.common.RPCConfig;
import org.acm.reducemap.worker.AssignWorkReply;
import org.acm.reducemap.worker.AssignWorkRequest;

import java.io.*;
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
    private HashMap<Integer,AssignType> compMap = new HashMap<>();
    private int totalJobs = 0;

//    private int result = 0; //demo

    class JobType {
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

    class AssignType {
        private int jobId;
        private int jobType;
        private int workerId;
        AssignType(int ji,int jt,int wi) {
            jobId = ji;
            jobType = jt;
            workerId = wi;
        }
    }

    JobScheduler(Master mas, Logger log) {
        master = mas;
        logger = log;
    }

    private void onJobComplete(int workerId, int jobId, int jobType, AssignWorkReply reply) {
        int status = reply.getStatus();
        System.out.println("get AssignWork reply id:"+workerId+" status:"+status);
//        result += status;
        master.workerMan.freeWorker(workerId);
        compMap.put(jobId,new AssignType(jobId,jobType,workerId));
        if (isAllComplete()) {
//            System.out.println("complete:"+result); //338350
            mergeResult();
            master.onComplete();
        }
    }

    private void mergeResult() {
        System.out.println("Merging result:");
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("./merge.out"));
            for (int i = 1; i <= compMap.size(); ++i) {
                AssignType asgn = compMap.get(i);
                if (asgn==null) continue;
                int workerId = asgn.workerId;
                int jobType = asgn.jobType;
                int jobId = asgn.jobId;
                String filename = "./output_"+workerId+"_"+jobId+"_"+jobType+".out";
                System.out.println("from:"+filename);
                bw.write("from:"+filename);
                bw.newLine();
                BufferedReader br = new BufferedReader(new FileReader(filename));
                String t;
                while((t=br.readLine())!=null){
                    bw.write(t);
                    System.out.println(t);
                    bw.newLine();
                }
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean isAllComplete() {
        return compMap.size() == totalJobs;
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

        totalJobs = jobQueue.size();
        JobType job;
        while ((job = getJob())!= null){
            int jobId = job.SerialNum;
            int jobType = job.WorkType;
            logger.info("Schedule:AssignWork:"+job.SerialNum);
            Vector<WorkerManager.workerInfo> avail = new Vector<>();
            while (true) {
                while (true){
                    master.workerMan.getAvailableWorkers(avail);
                    if (!avail.isEmpty()) break;
//                    System.out.println("no worker available, retrying...");
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
                StreamObserver<AssignWorkReply> replyObserver = new StreamObserver<AssignWorkReply>() {
                    int wid = workerId;
                    int jid = jobId;
                    int jtype = jobType;

                    @Override
                    public void onNext(AssignWorkReply value) {
                        onJobComplete(wid, jid, jtype, value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.log(Level.WARNING,"Assign work Failed: {0}", ((StatusRuntimeException)t).getStatus());
                    }

                    @Override
                    public void onCompleted() {}
                };
                succ = rpc.asyncCall("assignWork",
                        AssignWorkRequest.newBuilder().setWorkId(jobId).setWorkType(jobType).setParamHandle(job.para).build(),replyObserver);

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
