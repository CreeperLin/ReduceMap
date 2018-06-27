package org.acm.reducemap.master;

import org.acm.reducemap.common.RPCAddress;
import org.acm.reducemap.common.RPCConfig;
import org.acm.reducemap.worker.HaltWorkerReply;
import org.acm.reducemap.worker.HaltWorkerRequest;

import java.util.HashMap;
import java.util.Vector;

public class WorkerManager {

    public class workerInfo {

        boolean isAlive;
        boolean isBusy;
        int workerId;
        JobScheduler.JobType curJob;
        long lastAlive;
        long lastAssigned;
        MasterRPCClient cli;

        workerInfo(RPCAddress addr, int id) {
            isAlive = true;
            workerId = id;
            lastAlive = System.currentTimeMillis();
            cli = new MasterRPCClient(addr.hostname,addr.port);
        }

        @Override
        public String toString() {
            return "(Id:" + workerId + " alive:" + isAlive + " busy:" + isBusy + " workId:" + curJob.getNum() + " lastAlive:" + lastAlive + " lastAssign:" + lastAssigned + ")";
        }
    }

    private int curWorkerId = 0; //demo
    private HashMap<String, Integer> addressMap = new HashMap<>();
    private HashMap<Integer,workerInfo> workerMap = new HashMap<>();

    //demo
    synchronized private int getNewWorkerId() {
        return ++curWorkerId;
    }

    synchronized int registerWorker(String ipAddr) {
        Integer id = addressMap.get(ipAddr);
        if (id!=null) {
            System.out.println("WorkerMan:Re register worker:"+id);
            keepAliveWorker(id, ipAddr);
            return id;
        }
        int newId = getNewWorkerId();
        workerMap.put(newId,new workerInfo(new RPCAddress(ipAddr), newId));
        addressMap.put(ipAddr,newId);
        return newId;
    }

    synchronized void keepAliveWorker(int id, String ipAddr) {
        workerInfo wk = workerMap.get(id);
        if (wk == null) {
            System.out.println("WorkerMan:rediscover worker:"+id);
            wk = new workerInfo(new RPCAddress(ipAddr), id);
            workerMap.put(id,wk);
            addressMap.put(ipAddr, id);
        }
        wk.lastAlive = System.currentTimeMillis();
        wk.isAlive = true;
    }

    synchronized void downWorker(int id) {
        workerInfo wk = workerMap.get(id);
        if (wk == null) return;
        wk.isAlive = false;
    }

    synchronized void upWorker(int id) {
        workerInfo wk = workerMap.get(id);
        if (wk == null) return;
        wk.isAlive = true;
    }

    synchronized void busyWorker(int workerId, JobScheduler.JobType job) {
        workerInfo wk = workerMap.get(workerId);
        if (wk == null || !wk.isAlive) return;
        wk.isBusy = true;
        wk.curJob = job;
        wk.lastAssigned = System.currentTimeMillis();
        System.out.println("busy worker:"+workerId);
    }

    synchronized void freeWorker(int id) {
        workerInfo wk = workerMap.get(id);
        if (wk == null || !wk.isAlive) return;
        wk.isBusy = false;
        wk.curJob = null;
        System.out.println("freed worker:"+id);
    }

    synchronized void getAliveWorkers(Vector<workerInfo> ret) {
        ret.clear();
        workerMap.forEach((i,j)->{
            if (j.isAlive) {
                ret.add(j);
            }
        });
    }

    synchronized void getAvailableWorkers(Vector<workerInfo> ret) {
        ret.clear();
        workerMap.forEach((i,j)->{
            if (j.isAlive && !j.isBusy) {
                ret.add(j);
            }
        });
    }

    synchronized Vector<workerInfo> getAllWorkers() {
        return new Vector<>(workerMap.values());
    }

    synchronized Vector<workerInfo> getDeadWorkers() {
        Vector<workerInfo> ret = new Vector<>();
        long curTime = System.currentTimeMillis();
        workerMap.forEach((i,j)->{
            if (!j.isAlive) {
                ret.add(j);
            } else if (curTime-j.lastAlive>RPCConfig.workerDeadTimeout) {
                j.isAlive = false;
                ret.add(j);
            }
        });
        return ret;
    }

    synchronized Vector<workerInfo> getOverdueWorkers() {
        Vector<workerInfo> ret = new Vector<>();
        long curTime = System.currentTimeMillis();
        workerMap.forEach((i,j)->{
//            System.out.println("workerInfo:"+j);
            if (j.isAlive && j.isBusy && (curTime-j.lastAssigned)>RPCConfig.workerOverdueTimeout) {
                ret.add(j);
//                System.out.println("add:"+i);
            }
        });
        return ret;
    }

    synchronized MasterRPCClient getWorkerRPC(int id) {
        workerInfo wk = workerMap.get(id);
        if (wk == null) return null;
        return wk.cli;
    }

    synchronized void haltAllWorker() {
        workerMap.forEach((i,j)->{
            if (j.isAlive) {
                try {
                    HaltWorkerReply reply = (HaltWorkerReply)
                            j.cli.call("haltWorker",HaltWorkerRequest.newBuilder().setReason(1).build());
                    if (reply!=null) System.out.println("WorkerMan: worker halted: id:"+i);
                    j.cli.shutdown();
                } catch (InterruptedException ignored){}
            }
        });
    }
}
