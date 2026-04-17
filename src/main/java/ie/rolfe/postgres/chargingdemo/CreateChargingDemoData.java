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

public class CreateChargingDemoData extends BaseChargingDemo {

    /**
     * @param args
     */
    public static void main(String[] args) {

        Gson gson = new Gson();
        Random r = new Random();

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 6) {
            msg("Usage: hostnames recordcount tpms  maxinitialcredit username password");
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
        try {
            Connection mainClient = connectPG(hostlist, username, password);

            upsertAllUsers(userCount, tpMs, loblength, gson, initialCredit, mainClient);

            msg("Closing connection...");
            mainClient.close();

        } catch (Exception e) {
            msg(e.getMessage());
        }

    }

}
