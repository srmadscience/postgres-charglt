/*
 * Copyright (C) 2025 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package ie.rolfe.postgres.chargingdemo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static ie.rolfe.postgres.chargingdemo.ReferenceData.*;
import static org.junit.jupiter.api.Assertions.fail;

class DDLTest {


    private static final String TEST_JSON_REPLACE = "NEW_LOYALTY_NUMBER";
    private static final String TEST_JSON_OBJECT = "{}";
    final long NEW_USER_ID = 43;
    final long TEST_USER_ID = 42;
    final long BAD_USER_ID = 2;
    final long TEST_SESSION_ID = 12;
    final long BAD_SESSION_ID = 13;
    final long TEST_BALANCE = 1000;
    final long TEST_EXTRA_CREDIT = 333;
    final long TEST_ALLOCATED = 10;
    final long TEST_WANTED = 20;
    final long TEST_OVERSPEND = 100000;
    final String[] setupDML = {"INSERT INTO user_table  " +
            "(userid ,user_json_object ,user_last_seen ,user_softlock_sessionid ,user_softlock_expiry, user_balance)" +
            " VALUES " +
            "(" + TEST_USER_ID + ", NULL, NOW(), NULL, NULL, " + TEST_BALANCE + ");"
            , "INSERT INTO user_recent_transactions " +
            "(userid,user_txn_id,txn_time ,sessionid,approved_amount ,spent_amount ,purpose)" +
            " VALUES " +
            "(" + TEST_USER_ID + ",'Create1',NOW()," + TEST_SESSION_ID + ", 0, " + TEST_ALLOCATED + ", 'Setup');"
            , "INSERT INTO user_usage_table (userid,allocated_amount ,sessionid ,lastdate )" +
            " VALUES " +
            "(" + TEST_USER_ID + "," + TEST_ALLOCATED + "," + TEST_SESSION_ID + ", NOW());"};
    final String[] tearDownTables = {"delete from user_table", "delete from user_recent_transactions", "delete from user_usage_table"};
    Connection connection = null;

    @BeforeEach
    void setUp() {
        BaseChargingDemo.msg("setup");
        String password = System.getenv("PG_PASSWORD");
        if (password == null) {
            password = "xraytunnel";
        }
        String host = System.getenv("PG_HOST");
        if (host == null) {
            host = "10.13.1.30";
        }
        String user = System.getenv("PG_USER");
        if (user == null) {
            user = "root";
        }

        final String connectString = "jdbc:postgresql://" + host + ":5432/charglt?user=" + user + "&password=" + password;
        try {
            BaseChargingDemo.msg("Connecting to " + connectString);
            connection = DriverManager.getConnection(connectString);
            connection.setAutoCommit(false);

            for (int i = 0; i < tearDownTables.length; i++) {

                BaseChargingDemo.msg(tearDownTables[i]);
                PreparedStatement ps = connection.prepareStatement(tearDownTables[i]);
                ps.executeUpdate();
            }

            for (int i = 0; i < setupDML.length; i++) {

                BaseChargingDemo.msg(setupDML[i]);
                PreparedStatement ps = connection.prepareStatement(setupDML[i]);
                ps.executeUpdate();
            }

            connection.commit();

        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
        }
    }

    @AfterEach
    void tearDown() {
        BaseChargingDemo.msg("teardown");
        try {
            if (connection != null) {

                for (int i = 0; i < tearDownTables.length; i++) {

                    BaseChargingDemo.msg(tearDownTables[i]);
                    PreparedStatement ps = connection.prepareStatement(tearDownTables[i] + ";");
                    ps.executeUpdate();
                }


                connection.commit();
                connection.close();
            }
        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Test
    void Smoketest() {
        BaseChargingDemo.msg("smoketest");
        //setUp();
        try {
            PreparedStatement ps = connection.prepareStatement("SELECT * FROM user_table");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                long userid = rs.getLong("USERID");
                BaseChargingDemo.msg(this.getClass().getName() + ":Found " + userid);
            }
        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

        //tearDown();
    }

    @Test
    void GetUsersWithMultipleSessions() {
        BaseChargingDemo.msg("getUsersWithMultipleSessions");
        try {
            PreparedStatement cs = connection.prepareStatement("SELECT * FROM GetUsersWithMultipleSessions()");
            cs.execute();
            ResultSet rs = cs.getResultSet();

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                long userid = rs.getLong("USERID");
                BaseChargingDemo.msg(this.getClass().getName() + ":Found " + userid);
            }

            if (rowCount > 0) {
                fail("getUsersWithMultipleSessions: rows found");
            }

            PreparedStatement psExtraSession = connection.prepareStatement("INSERT INTO user_usage_table (userid,allocated_amount ,sessionid ,lastdate )" +
                    " VALUES " +
                    "(" + TEST_USER_ID + "," + TEST_ALLOCATED + "," + TEST_SESSION_ID + 1 + ", NOW());");
            psExtraSession.executeUpdate();

            cs.execute();
            rs = cs.getResultSet(); // re-execute after inserting extra session

            rowCount = 0;
            while (rs.next()) {
                rowCount++;
                long userid = rs.getLong("USERID");
                BaseChargingDemo.msg(this.getClass().getName() + ":Found " + userid);
            }

            if (rowCount != 1) {
                fail("getUsersWithMultipleSessions: mo rows found");
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void ShowTransactions() {
        BaseChargingDemo.msg("ShowTransactions");
        try {
            PreparedStatement cs = connection.prepareStatement("SELECT * FROM showTransactions(?)");
            cs.setLong(1, TEST_USER_ID);
            cs.execute();
            ResultSet rs = cs.getResultSet();

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                long userid = rs.getLong("USERID");
                BaseChargingDemo.msg(this.getClass().getName() + ":Found " + userid);
            }


            if (rowCount != 1) {
                fail("getUsersWithMultipleSessions: 1 row should have been found");
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void ShowTransactionsBadUser() {
        BaseChargingDemo.msg("ShowTransactions");
        try {
            PreparedStatement cs = connection.prepareStatement("SELECT * FROM showTransactions(?)");
            cs.setLong(1, TEST_USER_ID + 1);
            cs.execute();
            ResultSet rs = cs.getResultSet();

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                long userid = rs.getLong("USERID");
                BaseChargingDemo.msg(this.getClass().getName() + ":Found " + userid);
            }


            if (rowCount != 0) {
                fail("getUsersWithMultipleSessions: 0 row should have been found");
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void GetUser() {
        BaseChargingDemo.msg("GetUser");
        try {
            PreparedStatement cs = connection.prepareStatement("SELECT * FROM GetUser(?)");
            cs.setLong(1, TEST_USER_ID);
            cs.execute();

            int rowCount = 0;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null) {
                        while (resultSet.next()) {
                            rowCount++;
                            long userid = resultSet.getLong("userid");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + userid);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (rowCount != 3) {
                fail("getUsersWithMultipleSessions: 3 rows should have been found");
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void GetAndLockUserNoUser() {
        BaseChargingDemo.msg("GetAndLockUserNoUser");
        try {
            PreparedStatement cs = connection.prepareStatement("SELECT * FROM GetAndLockUser(?,?)");
            cs.setLong(1, BAD_USER_ID);
            cs.setLong(2, TEST_SESSION_ID);
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_USER_DOESNT_EXIST) {
                fail("GetAndLockUserNoUser: return code should be " + STATUS_USER_DOESNT_EXIST + ". " + " got " + l_status_byte);
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void GetAndLockUserRealUser() {
        BaseChargingDemo.msg("GetAndLockUserRealUser");
        try {

            //
            // make sure is unlocked...
            long lockingSessionId = getLongFromUserTable("user_softlock_sessionid");

            if (lockingSessionId != Long.MIN_VALUE) {
                fail("GetAndLockUserRealUser: Saw " + lockingSessionId + ", expected null ");
            }


            PreparedStatement cs = connection.prepareStatement("SELECT * FROM GetAndLockUser(?,?)");
            cs.setLong(1, TEST_USER_ID);
            cs.setLong(2, TEST_SESSION_ID);
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_RECORD_HAS_BEEN_SOFTLOCKED) {
                fail("GetAndLockUserRealUser: return code should be " + STATUS_RECORD_HAS_BEEN_SOFTLOCKED + ". " + " got " + l_status_byte);
            }


            //
            // See if our session ID is now there...
            lockingSessionId = getLongFromUserTable("user_softlock_sessionid");

            if (lockingSessionId != TEST_SESSION_ID) {
                fail("GetAndLockUserRealUser: Saw " + lockingSessionId + ", expected " + TEST_SESSION_ID);
            }

        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void UpdateUserNoUser() {
        BaseChargingDemo.msg("UpdateUserNoUser");
        try {
            PreparedStatement cs = connection.prepareStatement("SELECT * FROM UpdateLockedUser(?,?,?,?)");
            cs.setLong(1, BAD_USER_ID);
            cs.setLong(2, TEST_SESSION_ID);
            cs.setString(3, TEST_JSON_OBJECT);
            cs.setString(4, TEST_JSON_REPLACE);
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_USER_DOESNT_EXIST) {
                fail("UpdateUserNoUser: return code should be " + STATUS_USER_DOESNT_EXIST + ". " + " got " + l_status_byte);
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void AddCreditNoUser() {
        BaseChargingDemo.msg("AddCreditNoUser");
        try {
            PreparedStatement cs = connection.prepareStatement("SELECT * FROM AddCredit(?,?,?)");
            cs.setLong(1, BAD_USER_ID);
            cs.setLong(2, TEST_EXTRA_CREDIT);
            cs.setString(3, "Add Credit 1");
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_USER_DOESNT_EXIST) {
                fail("AddCreditNoUser: return code should be " + STATUS_USER_DOESNT_EXIST + ". " + " got " + l_status_byte);
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void ReportQuotaUsageNoUser() {
        BaseChargingDemo.msg("ReportQuotaUsageNoUser");
        try {
            PreparedStatement cs = connection.prepareStatement("SELECT * FROM ReportQuotaUsage(?,?,?,?,?)");
            cs.setLong(1, BAD_USER_ID);
            cs.setLong(2, 10);
            cs.setLong(3, 10);
            cs.setLong(4, 10);
            cs.setString(5, "No User");
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_USER_DOESNT_EXIST) {
                fail("ReportQuotaUsageNoUser: return code should be " + STATUS_USER_DOESNT_EXIST + ". " + " got " + l_status_byte);
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void ReportQuotaUsageRealUserNegCredit() {
        BaseChargingDemo.msg("ReportQuotaUsageRealUserNegCredit");
        try {

            final long TEST_SPEND = 1;

            //
            // make sure balance is TEST_BALANCE
            long check_balance = getLongFromUserTable("user_balance");

            if (check_balance != TEST_BALANCE) {
                fail("ReportQuotaUsageRealUserNegCredit: Saw " + check_balance + ", expected " + TEST_BALANCE);
            }

            PreparedStatement cs = connection.prepareStatement("SELECT * FROM ReportQuotaUsage(?,?,?,?,?)");
            cs.setLong(1, TEST_USER_ID);
            cs.setLong(2, TEST_OVERSPEND);
            cs.setLong(3, TEST_WANTED);
            cs.setLong(4, TEST_SESSION_ID);
            cs.setString(5, "Neg Credit");
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            connection.commit();

            if (l_status_byte != STATUS_NO_MONEY) {
                fail("ReportQuotaUsageRealUserNegCredit: return code should be " + STATUS_NO_MONEY + ". " + " got " + l_status_byte);
            }

            //
            // make sure balance is TEST_BALANCE - TEST_OVERSPEND
            check_balance = getLongFromUserTable("user_balance");

            if (check_balance != (TEST_BALANCE - TEST_OVERSPEND)) {
                fail("ReportQuotaUsageRealUserNegCredit: Saw " + check_balance + ", expected " + (TEST_BALANCE - TEST_OVERSPEND));
            }

            //
            // make sure allocated is Long.MIN_VALUE (null).
            long check_allocated = getLongFromUserTable("allocated_amount");

            if (check_allocated != Long.MIN_VALUE) {
                fail("ReportQuotaUsageRealUserNegCredit: Saw " + check_balance + ", expected null");
            }

        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void ReportQuotaUsageRealUserSomeUnits() {
        BaseChargingDemo.msg("ReportQuotaUsageRealUserSomeUnits");
        try {

            final long TEST_SPEND = 1;

            //
            // make sure balance is TEST_BALANCE
            long check_balance = getLongFromUserTable("user_balance");

            if (check_balance != TEST_BALANCE) {
                fail("ReportQuotaUsageRealUserSomeUnits: Saw " + check_balance + ", expected " + TEST_BALANCE);
            }

            PreparedStatement cs = connection.prepareStatement("SELECT * FROM ReportQuotaUsage(?,?,?,?,?)");
            cs.setLong(1, TEST_USER_ID);
            cs.setLong(2, TEST_SPEND);
            cs.setLong(3, TEST_BALANCE * 10);
            cs.setLong(4, TEST_SESSION_ID);
            cs.setString(5, "Some Units");
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            connection.commit();

            if (l_status_byte != STATUS_SOME_UNITS_ALLOCATED) {
                fail("ReportQuotaUsageRealUserSomeUnits: return code should be " + STATUS_SOME_UNITS_ALLOCATED + ". " + " got " + l_status_byte);
            }

            //
            // make sure balance is TEST_BALANCE - TEST_SPEND
            check_balance = getLongFromUserTable("user_balance");

            if (check_balance != (TEST_BALANCE - TEST_SPEND)) {
                fail("ReportQuotaUsageRealUserSomeUnits: Saw " + check_balance + ", expected " + (TEST_BALANCE - TEST_SPEND));
            }

            //
            // make sure allocated is 999
            long check_allocated = getLongFromUserTable("allocated_amount");

            if (check_allocated != (TEST_BALANCE - TEST_SPEND)) {
                fail("ReportQuotaUsageRealUserNegCredit: Saw " + check_balance + ", expected " + (TEST_BALANCE - TEST_SPEND));
            }

        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }


    @Test
    void ReportQuotaUsageRealAllUnitsAllocated() {
        BaseChargingDemo.msg("ReportQuotaUsageRealUserNegCredit");
        try {

            final long TEST_SPEND = 1;

            //
            // make sure balance is TEST_BALANCE
            long check_balance = getLongFromUserTable("user_balance");

            if (check_balance != TEST_BALANCE) {
                fail("ReportQuotaUsageRealUserNegCredit: Saw " + check_balance + ", expected " + TEST_BALANCE);
            }

            PreparedStatement cs = connection.prepareStatement("SELECT * FROM ReportQuotaUsage(?,?,?,?,?)");
            cs.setLong(1, TEST_USER_ID);
            cs.setLong(2, TEST_SPEND);
            cs.setLong(3, TEST_ALLOCATED);
            cs.setLong(4, TEST_SESSION_ID);
            cs.setString(5, "All Units");
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            connection.commit();

            if (l_status_byte != STATUS_ALL_UNITS_ALLOCATED) {
                fail("ReportQuotaUsageRealUserNegCredit: return code should be " + STATUS_ALL_UNITS_ALLOCATED + ". " + " got " + l_status_byte);
            }

            //
            // make sure balance is TEST_BALANCE - TEST_SPEND
            check_balance = getLongFromUserTable("user_balance");

            if (check_balance != (TEST_BALANCE - TEST_SPEND)) {
                fail("ReportQuotaUsageRealUserNegCredit: Saw " + check_balance + ", expected " + (TEST_BALANCE - TEST_SPEND));
            }

            //
            // make sure allocated is 0
            long check_allocated = getLongFromUserTable("allocated_amount");

            if (check_allocated != TEST_ALLOCATED) {
                fail("ReportQuotaUsageRealUserNegCredit: Saw " + check_balance + ", expected " + TEST_ALLOCATED);
            }

        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }


    @Test
    void AddCreditRealUser() {
        BaseChargingDemo.msg("AddCreditRealUser");
        final String ADD_CREDIT_TX = "AddCredit1";
        try {
            PreparedStatement cs = connection.prepareStatement("SELECT * FROM AddCredit(?,?,?)");
            cs.setLong(1, TEST_USER_ID);
            cs.setLong(2, TEST_EXTRA_CREDIT);
            cs.setString(3, ADD_CREDIT_TX);
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_CREDIT_ADDED) {
                fail("AddCreditRealUser: return code should be " + STATUS_CREDIT_ADDED + ". " + " got " + l_status_byte);
            }

            //
            // See if our balance has updated...
            long newBalance = getLongFromUserTable("user_balance");

            if (newBalance != TEST_EXTRA_CREDIT + TEST_BALANCE) {
                fail("UpdateUserRealUser: Saw " + newBalance + ", expected " + (TEST_EXTRA_CREDIT + TEST_BALANCE));
            }

            //
            // Try transaction again
            //
            cs.setLong(1, TEST_USER_ID);
            cs.setLong(2, TEST_EXTRA_CREDIT);
            cs.setString(3, ADD_CREDIT_TX);
            cs.execute();

            rowCount = 0;
            l_status_byte = Long.MIN_VALUE;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_TXN_ALREADY_HAPPENED) {
                fail("AddCreditRealUser: return code should be " + STATUS_TXN_ALREADY_HAPPENED + ". " + " got " + l_status_byte);
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }


    @Test
    void UpdateUserRealUser() {
        BaseChargingDemo.msg("UpdateUserRealUser");
        try {

            //
            // make sure is unlocked...
            long lockingSessionId = getLongFromUserTable("user_softlock_sessionid");

            if (lockingSessionId != Long.MIN_VALUE) {
                fail("GetAndLockUserRealUser: Saw " + lockingSessionId + ", expected null ");
            }


            PreparedStatement cs = connection.prepareStatement("SELECT * FROM UpdateLockedUser(?,?,?,?)");
            cs.setLong(1, TEST_USER_ID);
            cs.setLong(2, TEST_SESSION_ID);
            cs.setString(3, TEST_JSON_OBJECT);
            cs.setString(4, TEST_JSON_REPLACE);
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;


            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_OK) {
                fail("UpdateUserRealUser: return code should be " + STATUS_OK + ". " + " got " + l_status_byte);
            }


            //
            // See if our session ID is now there...
            lockingSessionId = getLongFromUserTable("user_softlock_sessionid");

            if (lockingSessionId != Long.MIN_VALUE) {
                fail("UpdateUserRealUser: Saw " + lockingSessionId + ", expected " + Long.MIN_VALUE);
            }

        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void UpdateUserWrongSessionId() {
        BaseChargingDemo.msg("UpdateUserRealUser");
        try {

            PreparedStatement cs = connection.prepareStatement("SELECT * FROM GetAndLockUser(?,?)");
            cs.setLong(1, TEST_USER_ID);
            cs.setLong(2, BAD_SESSION_ID);
            cs.execute();
            connection.commit();

            //
            // make sure is unlocked...
            long lockingSessionId = getLongFromUserTable("user_softlock_sessionid");

            if (lockingSessionId != BAD_SESSION_ID) {
                fail("GetAndLockUserRealUser: Saw " + lockingSessionId + ", expected " + BAD_SESSION_ID);
            }


            cs = connection.prepareStatement("SELECT * FROM UpdateLockedUser(?,?,?,?)");
            cs.setLong(1, TEST_USER_ID);
            cs.setLong(2, TEST_SESSION_ID);
            cs.setString(3, TEST_JSON_OBJECT);
            cs.setString(4, TEST_JSON_REPLACE);
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;


            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_RECORD_ALREADY_SOFTLOCKED) {
                fail("UpdateUserRealUser: return code should be " + STATUS_RECORD_ALREADY_SOFTLOCKED + ". " + " got " + l_status_byte);
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }


    @Test
    void DelUser() {
        BaseChargingDemo.msg("DelUser");
        try {

            long userId = getLongFromUserTable("userid");

            if (userId == Long.MIN_VALUE) {
                fail("DelUser: User not found at start");
            }


            PreparedStatement cs = connection.prepareStatement("SELECT DelUser(?)");
            cs.setLong(1, TEST_USER_ID);
            cs.execute();

        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }


        long userId = getLongFromUserTable("userid");

        if (userId != Long.MIN_VALUE) {
            fail("DelUser: User still found at end");
        }


    }


    @Test
    void UpsertUserExistingUser() {
        BaseChargingDemo.msg("UpsertUserExistingUser");
        try {

            final String TEST_TXN_ID = "Test TXN UpsertUser";
            // make sure exists...
            long existingUserid = getLongFromUserTable("userid");

            if (existingUserid != TEST_USER_ID) {
                fail("UpsertUserExistingUser: Test user not found at start");
            }

            // make sure balance is TEST_BALANCE...
            long existing_balance = getLongFromUserTable("user_balance");

            if (existing_balance != TEST_BALANCE) {
                fail("UpsertUserExistingUser: Test balance wrong at start");
            }

            PreparedStatement cs = connection.prepareStatement("SELECT * FROM UpsertUser(?,?,?,?,?,?)");
            cs.setLong(1, TEST_USER_ID);
            cs.setLong(2, TEST_BALANCE * 2);
            cs.setString(3, TEST_JSON_OBJECT);
            cs.setString(4, "Test: UpsertUserExistingUser");
            cs.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            cs.setString(6, TEST_TXN_ID);
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;


            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_OK) {
                fail("UpsertUserExistingUser: return code should be " + STATUS_OK + ". " + " got " + l_status_byte);
            }


            // make sure balance is TEST_BALANCE * 2...
            existing_balance = getLongFromUserTable("user_balance");

            if (existing_balance != TEST_BALANCE * 2) {
                fail("UpsertUserExistingUser: Test balance wrong at end");
            }

            connection.commit();

            //
            // Now try same TX again, but with 3x balance
            //
            cs.setLong(1, TEST_USER_ID);
            cs.setLong(2, TEST_BALANCE * 3); // <- NOTE
            cs.setString(3, TEST_JSON_OBJECT);
            cs.setString(4, "Test: UpsertUserExistingUser");
            cs.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            cs.setString(6, TEST_TXN_ID);
            cs.execute();

            rowCount = 0;
            l_status_byte = Long.MIN_VALUE;


            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_TXN_ALREADY_HAPPENED) {
                fail("UpsertUserExistingUser: return code should be " + STATUS_TXN_ALREADY_HAPPENED + ". " + " got " + l_status_byte);
            }

            //
            // Sanity check - is balance still 2 * ?
            existing_balance = getLongFromUserTable("user_balance");

            if (existing_balance != TEST_BALANCE * 2) {
                fail("UpsertUserExistingUser: Test balance wrong at end " + existing_balance);
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }
    }


    @Test
    void UpsertUserNewUser() {
        BaseChargingDemo.msg("UpsertUserNewUser");
        try {

            final String TEST_TXN_ID = "Test TXN UpsertUser";
            // make sure exists...
            long existingUserid = getLongFromUserTable("userid", NEW_USER_ID, false);

            if (existingUserid != Long.MIN_VALUE) {
                fail("UpsertUserNewUser: Test user found at start");
            }

            PreparedStatement cs = connection.prepareStatement("SELECT * FROM UpsertUser(?,?,?,?,?,?)");
            cs.setLong(1, NEW_USER_ID);
            cs.setLong(2, TEST_BALANCE * 2);
            cs.setString(3, TEST_JSON_OBJECT);
            cs.setString(4, "Test: UpsertUserExistingUser");
            cs.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            cs.setString(6, TEST_TXN_ID);
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;


            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_OK) {
                fail("UpsertUserNewUser: return code should be " + STATUS_OK + ". " + " got " + l_status_byte);
            }


            // make sure balance is TEST_BALANCE * 2...
            long existing_balance = getLongFromUserTable("user_balance", NEW_USER_ID, false);

            if (existing_balance != TEST_BALANCE * 2) {
                fail("UpsertUserNewUser: Test balance wrong at end");
            }

            connection.commit();

            //
            // Now try same TX again, but with 3x balance
            //
            cs.setLong(1, NEW_USER_ID);
            cs.setLong(2, TEST_BALANCE * 3); // <- NOTE
            cs.setString(3, TEST_JSON_OBJECT);
            cs.setString(4, "Test: UpsertUserExistingUser");
            cs.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            cs.setString(6, TEST_TXN_ID);
            cs.execute();

            rowCount = 0;
            l_status_byte = Long.MIN_VALUE;


            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_TXN_ALREADY_HAPPENED) {
                fail("UpsertUserNewUser: return code should be " + STATUS_TXN_ALREADY_HAPPENED + ". " + " got " + l_status_byte);
            }

            //
            // Sanity check - is balance still 2 * ?
            existing_balance = getLongFromUserTable("user_balance", NEW_USER_ID, false);

            if (existing_balance != TEST_BALANCE * 2) {
                fail("UpsertUserNewUser: Test balance wrong at end " + existing_balance);
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }
    }


    private boolean hasColumn(ResultSet resultSet, String columnName) throws SQLException {

        java.sql.ResultSetMetaData rsmd = resultSet.getMetaData();

        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            if (rsmd.getColumnName(i).equalsIgnoreCase(columnName)) {
                return true;
            }
        }
        return false;
    }

    private long getLongFromUserTable(String fieldName) {
        return getLongFromUserTable(fieldName, TEST_USER_ID, false);
    }

    private long getLongFromUserTable(String fieldName, long userId, boolean addValues) {

        long value = Long.MIN_VALUE;
        long totalValue = Long.MIN_VALUE;

        try {
            PreparedStatement cs = connection.prepareStatement("SELECT * FROM GetUser(?)");
            cs.setLong(1, userId);
            cs.execute();

            int rowCount = 0;


            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, fieldName)) {
                        while (resultSet.next()) {
                            rowCount++;
                            value = resultSet.getLong(fieldName);
                            if (resultSet.wasNull()) {
                                value = Long.MIN_VALUE;
                            }
                            totalValue += value;
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

        if (addValues) {
            return totalValue;
        }
        return value;
    }

}