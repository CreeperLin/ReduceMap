package org.acm.reducemap.common;

public class RPCAddress {
    public String hostname;
    public int port;

    private static String toHostname(int ipAddr) {
        if (ipAddr==0) return "localhost";
        int mask = (1<<8) - 1;
        String sb = String.valueOf(mask & ipAddr >> 24) + '.' +
                (mask & ipAddr >> 16) + '.' +
                (mask & ipAddr >> 8) + '.' +
                (mask & ipAddr);
        return sb;
    }

    public RPCAddress(String addr) {
        String[] str = addr.split(":");
        if (str.length!=2) return;
        hostname = str[0];
        port = Integer.parseInt(str[1]);
    }

    public RPCAddress(int ipAddr, int p) {
        hostname = toHostname(ipAddr);
        port = p;
    }

    public RPCAddress(String ipAddr, int p) {
        hostname = ipAddr;
        port = p;
    }

}
