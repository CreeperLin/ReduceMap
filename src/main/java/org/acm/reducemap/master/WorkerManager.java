package org.acm.reducemap.master;

import org.acm.reducemap.common.RPCAddress;
import org.acm.reducemap.worker.HaltWorkerReply;
import org.acm.reducemap.worker.HaltWorkerRequest;

import java.util.HashMap;
import java.util.Vector;

public class WorkerManager {

    public class workerInfo {

        boolean isAlive;
        boolean isBusy;
        int workerId;
        long lastAlive;
        MasterRPCClient cli;

        workerInfo(RPCAddress addr, int id) {
            isAlive = true;
            workerId = id;
            lastAlive = System.currentTimeMillis()/1000;
            cli = new MasterRPCClient(addr.hostname,addr.port);
        }

    }

    private int curWorkerId = 0; //demo
    private HashMap<String, Integer> addressMap = new HashMap<>();
    private HashMap<Integer,workerInfo> workerMap = new HashMap<>();

    //demo
    synchronized private int getNewWorkerId() {
        return ++curWorkerId;
    }

    synchronized int registerWorker(RPCAddress addr) {
        Integer id = addressMap.get(addr.toString());
        if (id!=null) {
            System.out.println("WorkerMan:Re register worker:"+id);
            keepAliveWorker(id);
            return id;
        }
        int newId = getNewWorkerId();
        workerMap.put(newId,new workerInfo(addr, newId));
        addressMap.put(addr.toString(),newId);
        return newId;
    }

    synchronized void keepAliveWorker(int id) {
        workerInfo wk = workerMap.get(id);
        if (wk == null) return;
        wk.lastAlive = System.currentTimeMillis()/1000;
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

    synchronized void busyWorker(int id) {
        workerInfo wk = workerMap.get(id);
        if (wk == null || !wk.isAlive) return;
        wk.isBusy = true;
        System.out.println("busy worker:"+id);
    }

    synchronized void freeWorker(int id) {
        workerInfo wk = workerMap.get(id);
        if (wk == null || !wk.isAlive) return;
        wk.isBusy = false;
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

    synchronized MasterRPCClient getWorkerRPC(int id) {
        workerInfo wk = workerMap.get(id);
        if (wk == null) return null;
        return wk.cli;
    }

    void haltAllWorker() {
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
