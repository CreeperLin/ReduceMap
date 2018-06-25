package org.acm.reducemap.common;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class RPCConfig {

    public static int masterPort = 50051;
    public static int workerPort = 50052;

    public static long workerRegisterRetryInterval = 3000;
    public static long workerHeartbeatInterval = 5000;
    public static long workerHeartbeatRetryInterval = 3000;
    public static long workerDeadTimeout = 15000;
    public static long workerOverdueTimeout = 3000;

    public static long masterBackgroundInterval = 3000;
    public static long masterScheduleRetryInterval = 500;

    public static String getLocalIpAddr() throws SocketException {
        String ip="";
        for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
            NetworkInterface intf = en.nextElement();
            String name = intf.getName();
            if (!name.contains("docker") && !name.contains("lo")) {
                for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements();) {
                    InetAddress inetAddress = enumIpAddr.nextElement();
                    if (!inetAddress.isLoopbackAddress()) {
                        String ipaddress = inetAddress.getHostAddress();
                        if (!ipaddress.contains("::") && !ipaddress.contains("0:0:") && !ipaddress.contains("fe80")) {
                            System.out.println(ipaddress);
                            if(!"127.0.0.1".equals(ip)){
                                ip = ipaddress;
                            }
                        }
                    }
                }
            }
        }
        return ip;
    }

}
