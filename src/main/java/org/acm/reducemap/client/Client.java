package org.acm.reducemap.client;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class Client {

    private ClientRPCClient client; //rpc client
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private LinkedList<String> workLoad = new LinkedList<String>();

    private Client(String masterAddr, int masterPort) {
        client = new ClientRPCClient(masterAddr,masterPort);
    }

    private void run() throws InterruptedException{
        int workCnt = 0;
        NewWorkReply newWorkReply;
        ExecuteReply executeReply;
        while (!workLoad.isEmpty()) {
            NewWorkRequest newWorkRequest = NewWorkRequest.newBuilder().setName(workCnt).setExecHandle(workLoad.peekFirst()).build();
            while (true) {
                newWorkReply = client.newWork(newWorkRequest);
                if (newWorkReply!=null) break;
                logger.warning("newWork request operation failed, retrying...");
            }

            ExecuteRequest executeRequest = ExecuteRequest.newBuilder().setWorkType(1).setParamHandle(workLoad.peekFirst()).build();
            while (true) {
                newWorkReply = client.newWork(newWorkRequest);
                if (newWorkReply!=null) break;
                logger.warning("execute request operation failed, retrying...");
            }
            workLoad.removeFirst();
        }
        client.shutdown();
    }

    private NewWorkReply newWork(NewWorkRequest req) {
        return (NewWorkReply) client.call("newWork", req);
    }

    private ExecuteReply execute(ExecuteRequest req) {
        return (ExecuteReply) client.call("execute", req);
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length<2) {
            logger.warning("specify master address and port");
            return;
        }
        String masterAddr = args[0];
        int masterPort = Integer.parseInt(args[1]);

        for (int i = 2; i < args.size(); ++i)
            workLoad.add(args[i]);

        System.out.println("master addr:"+masterAddr+" port:"+masterPort);
        Client client = new Client(masterAddr,masterPort);
        client.run();
    }
}
