package org.acm.reducemap.master;

import org.acm.reducemap.common.RPCAddress;
import org.acm.reducemap.worker.HaltWorkerReply;

import java.util.HashMap;
import java.util.Vector;

public class WorkerManager {

    public class workerInfo {

        boolean isAlive;
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

    private HashMap<Integer,workerInfo> workerMap = new HashMap<>();

    synchronized void registerWorker(RPCAddress addr, int id) {
        workerMap.put(id,new workerInfo(addr, id));
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

    synchronized void getAliveWorkers(Vector<workerInfo> ret) {
        ret.clear();
        workerMap.forEach((i,j)->{
            if (j.isAlive) {
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
                    HaltWorkerReply reply = j.cli.haltWorker(1);
                    if (reply!=null) System.out.println("WorkerMan: worker halted: id:"+i);
                    j.cli.shutdown();
                } catch (InterruptedException ignored){}
            }
        });
    }
}
