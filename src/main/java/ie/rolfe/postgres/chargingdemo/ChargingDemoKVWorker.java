package ie.rolfe.postgres.chargingdemo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ChargingDemoKVWorker extends BaseChargingDemo implements Runnable, AutoCloseable {

    public static final int MAX_QUEUE_SIZE = 10;
    private static final long POLL_MS = 100;
    final ArrayBlockingQueue<KVRequest> requestQueue = new ArrayBlockingQueue<KVRequest>(MAX_QUEUE_SIZE);
    int workerId;
    Connection mainConnection;
    Random r;
    PreparedStatement getAndLock;
    PreparedStatement updateLockedUser;
    UserKVState[] users;
    volatile boolean keepGoing = true;


    public ChargingDemoKVWorker(int workerId, String hostlist, String username, String password, Random r, UserKVState[] users) {
        this.workerId = workerId;
        this.r = r;
        this.users = users;
        try {
            mainConnection = connectPG(hostlist, username, password);
            getAndLock = mainConnection.prepareStatement("SELECT * FROM GetAndLockUser(?,?)");
            updateLockedUser = mainConnection.prepareStatement("SELECT * FROM UpdateLockedUser(?,?,?,?)");
        } catch (Exception e) {
            throw new RuntimeException(e);

        }
    }

    public void setKeepGoing(boolean keepGoing) {
        this.keepGoing = keepGoing;
        workerMsg("KeepGoing set to " + keepGoing + ". Queue size: " + requestQueue.size());
    }

    public boolean addRequest(KVRequest request) {

        boolean nowait = true;
        int attempts = 1;

        final long startMs = System.currentTimeMillis();

        // Block (rather than spin) until the worker has room for this request
        if (!requestQueue.offer(request)) {
            shc.incCounter("QUEUE_OVERFLOW");
            attempts++;
            nowait = false;
            try {
                requestQueue.put(request);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        shc.reportLatency(BaseChargingDemo.QUEUE_QUEUE, startMs, "Time spent waiting to join request queue", 10000);
        shc.report(BaseChargingDemo.ATTEMPTS, attempts, "Attempts needed to get into queue", 1000);
        return nowait;

    }

    @Override
    public void run() {

        while (keepGoing) {

            try {
                // Wait for work instead of polling an empty queue; the timeout lets us notice keepGoing
                KVRequest request = requestQueue.poll(POLL_MS, TimeUnit.MILLISECONDS);
                if (request != null) {

                    shc.reportLatency(BaseChargingDemo.QUEUE_LAG, request.createMS, "Time spent in request queue", 10000);
                    doKVtransaction(request.userCount, request.jsonsize, request.deltaProportion, request.userState, request.oursession, getAndLock
                            , request.startMs, r, updateLockedUser, request.gson);
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        try {
            mainConnection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void workerMsg(String message) {

        msg(workerId + ": " + message);

    }

    public void drain() {
        while (!requestQueue.isEmpty()) {

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
    }

    @Override
    public void close() throws Exception {
        if (mainConnection != null) {
            mainConnection.close();
        }
    }
}
