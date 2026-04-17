package ie.rolfe.postgres.chargingdemo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

public class ChargingDemoKVWorker extends BaseChargingDemo implements Runnable, AutoCloseable {

    public static final int MAX_QUEUE_SIZE = 10;
    private static final int SLEEP_NANOS = 5000;
    final ArrayList<KVRequest> requestQueue = new ArrayList<KVRequest>();
    int workerId;
    Connection mainConnection;
    Random r;
    PreparedStatement getAndLock;
    PreparedStatement updateLockedUser;
    UserKVState[] users;
    boolean keepGoing = true;


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
        int qSize = Integer.MAX_VALUE;

        int attempts = 1;

        final long startMs = System.currentTimeMillis();

        // Note that we don't need *perfect* synchronization. We just need to stop the queue
        // growing uncontrollably;
        synchronized (requestQueue) {
            qSize = requestQueue.size();
        }

        while (qSize >= MAX_QUEUE_SIZE) {
            shc.incCounter("QUEUE_OVERFLOW");
            attempts++;
            nowait = false;
            try {
                Thread.sleep(0, SLEEP_NANOS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            synchronized (requestQueue) {
                qSize = requestQueue.size();
            }

        }
        synchronized (requestQueue) {
            requestQueue.add(request);
        }

        shc.reportLatency(BaseChargingDemo.QUEUE_QUEUE, startMs, "Time spent waiting to join request queue", 10000);
        shc.report(BaseChargingDemo.ATTEMPTS, attempts, "Attempts needed to get into queue", 1000);
        return nowait;

    }

    @Override
    public void run() {

        while (keepGoing) {

            while (!requestQueue.isEmpty()) {
                try {
                    KVRequest request = null;
                    synchronized (requestQueue) {
                        request = requestQueue.removeFirst();
                    }

                    shc.reportLatency(BaseChargingDemo.QUEUE_LAG, request.createMS, "Time spent in request queue", 10000);
                    doKVtransaction(request.userCount, request.jsonsize, request.deltaProportion, request.userState, request.oursession, getAndLock
                            , request.startMs, r, updateLockedUser, request.gson);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                Thread.sleep(0, SLEEP_NANOS);
            } catch (InterruptedException e) {
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
                Thread.sleep(0, SLEEP_NANOS);
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
