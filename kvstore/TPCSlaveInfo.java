package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.*;
import java.util.regex.*;

/**
 * Data structure to maintain information about SlaveServers
 */
public class TPCSlaveInfo {

    private long slaveID;
    private String hostName;
    private int port;
    private static final Pattern SLAVE_INFO_REGEX = Pattern.compile("^(.*)@(.*):(.*)$");

    /**
     * Construct a TPCSlaveInfo to represent a slave server.
     *
     * @param info as "SlaveServerID@Hostname:Port"
     * @throws KVException ERROR_INVALID_FORMAT if info string is invalid
     */
    public TPCSlaveInfo(String slaveInfo) throws KVException {
        try {
            Matcher slaveInfoMatcher = SLAVE_INFO_REGEX.matcher(slaveInfo);

            if (!slaveInfoMatcher.matches()) {
                throw new IllegalArgumentException();
            }

            slaveID = Long.parseLong(slaveInfoMatcher.group(1));
            hostName = slaveInfoMatcher.group(2);
            port = Integer.parseInt(slaveInfoMatcher.group(3));
        } catch (Exception ex) {
            throw new KVException(new KVMessage(
                RESP, "Unknown Error: Could not parse slave info"));
        }
    }

    public long getSlaveID() {
        return slaveID;
    }

    public String getHostname() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    /**
     * Create and connect a socket within a certain timeout.
     *
     * @return Socket object connected to SlaveServer, with timeout set
     * @throws KVException ERROR_SOCKET_TIMEOUT, ERROR_COULD_NOT_CREATE_SOCKET,
     *         or ERROR_COULD_NOT_CONNECT
     */
    public Socket connectHost(int timeout) throws KVException {
        try {
            Socket sock = new Socket();
            sock.setSoTimeout(timeout);
            sock.connect(new InetSocketAddress(hostName, port), timeout);
            return sock;
        } catch (UnknownHostException ex) {
            throw new KVException(new KVMessage(
                RESP, "Network Error: Could not connect"));
        } catch (IOException ex) {
            throw new KVException(new KVMessage(
                RESP, "Network Error: Could not create socket"));
        }
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param sock Socket to be closed
     */
    public void closeHost(Socket sock) throws KVException {
        try {
            sock.close();
        } catch (IOException ex) {
            throw new KVException(new KVMessage(
                RESP, "Network Error: Could not close socket"));
        }
    }
}
