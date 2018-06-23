package org.acm.reducemap.master;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.acm.reducemap.worker.AssignWorkReply;
import org.acm.reducemap.worker.AssignWorkRequest;

import java.util.ArrayDeque;
import java.util.Random;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

class JobScheduler {

    private Master master;
    private Logger logger;
    private ArrayDeque<Integer> jobQueue = new ArrayDeque<>();

    JobScheduler(Master mas, Logger log) {
        master = mas;
        logger = log;
    }

    private void onJobComplete(AssignWorkReply reply) {
        int status = reply.getStatus();
        int workerId = reply.getWorkerId();
        System.out.println("get AssignWork reply id:"+workerId+" status:"+status);
        master.workerMan.freeWorker(workerId);
    }

    void schedule() throws InterruptedException{
        int t = 100;
        for (int i = 1;i<=t;++i) jobQueue.add(i);

        while (!jobQueue.isEmpty()){
            int jobId = jobQueue.pop();
            logger.info("Schedule:AssignWork:"+jobId);
            Vector<WorkerManager.workerInfo> avail = new Vector<>();
            while (true) {
                while (true){
                    master.workerMan.getAvailableWorkers(avail);
                    if (!avail.isEmpty()) break;
                    Thread.sleep(500);
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
                    @Override
                    public void onNext(AssignWorkReply value) {
                        onJobComplete(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.log(Level.WARNING,"RecordRoute Failed: {0}", ((StatusRuntimeException)t).getStatus());
                    }

                    @Override
                    public void onCompleted() {}
                };
                succ = rpc.asyncCall("assignWork",
                        AssignWorkRequest.newBuilder().setWorkId(jobId).setWorkType(jobId % 5).build(),replyObserver);

                if (!succ) {
                    logger.warning("worker down, retrying in 1s");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {}
                    continue;
                }
                master.workerMan.busyWorker(workerId);
                break;
            }
        }
        logger.info("Schedule:complete");
    }

}
