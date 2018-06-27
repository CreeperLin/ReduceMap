package org.acm.reducemap.client;

import org.acm.reducemap.master.ExecuteReply;
import org.acm.reducemap.master.ExecuteRequest;
import org.acm.reducemap.master.NewWorkReply;
import org.acm.reducemap.master.NewWorkRequest;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class Client {

    private ClientRPCClient client; //rpc client
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private Client(String masterAddr, int masterPort) {
        client = new ClientRPCClient(masterAddr,masterPort);
    }

    static List<String> Paths = new LinkedList<>();
    static List<String> Params = new LinkedList<>();

    private void demo(){
        for(int i = 0; i < 10; i++){
            Paths.add("./work" + i +".py");
            Params.add(" " + i + ";");
        }
    }

    private void run() throws InterruptedException{
        NewWorkReply reply;
        ExecuteReply reply1;
        for(int i = 0; i < Paths.size(); i++){
            reply = (NewWorkReply) client.call("newWork",
                    NewWorkRequest.newBuilder().setName("work" + i).setExecHandle(Paths.get(i)).build());

            int worktype = reply.getWorkType();
            do{
                reply1 = (ExecuteReply) client.call("execute",
                        ExecuteRequest.newBuilder().setWorkType(worktype).setParamHandle(Params.get(i)).build());
            }while(reply1.getStatus() != 0);
        }
        client.shutdown();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length<2) {
            logger.warning("specify master address and port");
            return;
        }
        String masterAddr = args[0];
        int masterPort = Integer.parseInt(args[1]);
        //demo
        Paths.add(args[2]);
        Params.add(args[3]);

        System.out.println("master addr:"+masterAddr+" port:"+masterPort);
        Client client = new Client(masterAddr,masterPort);
        client.run();
    }
}
