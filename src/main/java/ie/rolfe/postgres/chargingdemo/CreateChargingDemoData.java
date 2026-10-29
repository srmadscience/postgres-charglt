/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
/*
 * Copyright (C) 2025 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package ie.rolfe.postgres.chargingdemo;

import com.google.gson.Gson;
import java.sql.Connection;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class CreateChargingDemoData extends BaseChargingDemo {

    /**
     * @param args
     */
    public static void main(String[] args) {

        Gson gson = new Gson();
        Random r = new Random();

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 6 && args.length != 7) {
            msg("Usage: hostnames recordcount tpms  maxinitialcredit username password [threadcount]");
            System.exit(1);
        }

        // Comma delimited list of hosts...
        String hostlist = args[0];

        // How many users
        int userCount = Integer.parseInt(args[1]);

        // Target transactions per millisecond.
        int tpMs = Integer.parseInt(args[2]);

        // How long our arbitrary JSON payload will be.
        int loblength = 120;
        final String ourJson = getExtraUserDataAsJsonString(loblength, gson, r, userCount);

        // Default credit users are 'born' with
        int initialCredit = Integer.parseInt(args[3]);

        String username = args[4];
        String password = args[5];

        // Each thread has its own connection and loads every threadCount'th user, which
        // matters when every round trip is slow, e.g. to a remote managed database.
        int threadCount = args.length == 7 ? Integer.parseInt(args[6]) : 1;
        int tpMsPerThread = threadCount == 1 ? tpMs : Math.max(1, tpMs / threadCount);

        Thread[] threads = new Thread[threadCount];
        AtomicBoolean failed = new AtomicBoolean(false);
        final long startMs = System.currentTimeMillis();

        for (int t = 0; t < threadCount; t++) {
            final int firstUser = t;
            threads[t] = new Thread(() -> {
                try {
                    // Batch many upserts per transaction, so this loader manages its own commits.
                    Connection mainClient = connectPG(hostlist, username, password, false);

                    upsertUsers(firstUser, threadCount, userCount, tpMsPerThread, loblength, new Gson(),
                            initialCredit, mainClient);

                    msg("Closing connection...");
                    mainClient.close();

                } catch (Exception e) {
                    msg(e.getMessage());
                    failed.set(true);
                }
            }, "loader-" + t);
            threads[t].start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failed.set(true);
            }
        }

        long elapsedMs = Math.max(1, System.currentTimeMillis() - startMs);
        msg("Loaded " + userCount + " users with " + threadCount + " threads in " + elapsedMs + "ms ("
                + (userCount * 1000L / elapsedMs) + " users per second)");

        if (failed.get()) {
            msg("At least one loader thread failed");
            System.exit(1);
        }

    }

}
