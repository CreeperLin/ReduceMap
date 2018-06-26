package org.acm.reducemap.client;

import java.io.IOException;
import java.util.logging.Logger;

public class Client {

    private ClientRPCClient client; //rpc client
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private Client(String masterAddr, int masterPort) {
        client = new ClientRPCClient(masterAddr,masterPort);
    }

    private void run() throws InterruptedException{

        client.shutdown();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length<2) {
            logger.warning("specify master address and port");
            return;
        }
        String masterAddr = args[0];
        int masterPort = Integer.parseInt(args[1]);
        System.out.println("master addr:"+masterAddr+" port:"+masterPort);
        Client client = new Client(masterAddr,masterPort);
        client.run();
    }
}
