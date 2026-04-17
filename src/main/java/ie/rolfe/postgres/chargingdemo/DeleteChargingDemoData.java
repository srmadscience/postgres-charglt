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

public class DeleteChargingDemoData extends BaseChargingDemo {

    /**
     * @param args
     */
    public static void main(String[] args) {

        Gson gson = new Gson();
        Random r = new Random();

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 5) {
            msg("Usage: hostnames recordcount tpms  username password");
            System.exit(1);
        }

        // Comma delimited list of hosts...
        String hostlist = args[0];

        // How many users
        int userCount = Integer.parseInt(args[1]);

        // Target transactions per millisecond.
        int tpMs = Integer.parseInt(args[2]);
        String username = args[3];
        String password = args[4];
        try {
            Connection mainClient = connectPG(hostlist, username, password);
            deleteAllUsers(0, userCount, tpMs, mainClient);

            msg("Closing connection...");
            mainClient.close();

        } catch (Exception e) {
            msg(e.getMessage());
        }

    }

}
