package com.ibi;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AppMethods {
    // Connection info
    final static String jdbcURL = "jdbc:postgresql://localhost:5432/exchangerates";
    final static String jdbcUsername = "postgres";
    final static String jdbcPassword = "ibibam123";
    final static String apiURL = "https://api.exchangerate.host/latest";

    private static Map<String, Double> cache = new HashMap<>();
    private static Connection jdbcConnect;
    private static HttpURLConnection apiConnect;
    final private static Logger logger = Logger.getLogger(AppMethods.class.getName());

    /**
     * Connects to the exchangerate.host API
     */
    public static void apiConnect() {
        try {
            URL url = new URL(apiURL);
            apiConnect = (HttpURLConnection) url.openConnection();
            apiConnect.connect();
            logger.info("API connection opened");
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error connecting to API", e);
            e.printStackTrace();
        }
    }

    /**
     * Disconnects from the exchangerate.host API
     */
    public static void apiDisconnect() {
        apiConnect.disconnect();
        logger.info("API connection closed");
    }

    /**
     * Connects to the Postgres database using jdbc
     */
    public static void pgConnect() {
        try {
            jdbcConnect = DriverManager.getConnection(jdbcURL, jdbcUsername, jdbcPassword);
            logger.info("PG connection opened");
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error connecting to PG", e);
            e.printStackTrace();
        }
    }

    /**
     * Disconnects from Postgres
     */
    public static void pgDisconnect() {
        try {
            jdbcConnect.close();
            logger.info("PG connection closed");
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error disconnecting from PG", e);
            e.printStackTrace();
        }
    }

    /**
     * Checks the number of active (connected) users on Postgres using
     * pg_stat_activity connection, used to determine overload
     * 
     * @return true if database is overloaded (threshold exceeded), otherwise false
     */
    private static boolean pgCheckLoad() {
        try {
            Connection connection = DriverManager.getConnection(jdbcURL, jdbcUsername, jdbcPassword);
            java.sql.Statement statement = connection.createStatement();
            ResultSet resultSet = statement
                    .executeQuery("SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'active'");
            if (resultSet.next()) {
                int active = resultSet.getInt(1);
                int threshold = 10; // Connections threshold before overload
                return active <= threshold;
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error checking database load", e);
            e.printStackTrace();
        }
        return false; // Assume cache usage if an error occurs
    }

    /**
     * Fetches rate associated with currency from Postgres using SQL querying, cache
     * is prioritised if the database is under heavy load.
     * 
     * @param currency the currency which is to be fetched from
     * @return the currency rate, or -1.0 if an error occurs
     */
    public static double pgFetch(String currency) {
        double rate;
        try {
            String sqlFetch = "SELECT * FROM rates.rates_table WHERE name = ?";
            PreparedStatement statement = jdbcConnect.prepareStatement(sqlFetch);
            if (cache.containsKey(currency) && pgCheckLoad()) {
                // Retrieve the rate from the cache if available and database is not overloaded
                rate = cache.get(currency);
                logger.info("Cache accessed fetching " + currency);
            } else {
                // Fetch the rate from the database and update the cache
                statement.setString(1, currency);
                ResultSet resultSet = statement.executeQuery();
                if (resultSet.next()) {
                    rate = resultSet.getDouble("rates");
                    cache.put(currency, rate);
                } else {
                    throw new Exception("Rate not found for " + currency);
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error fetching rate", e);
            e.printStackTrace();
            return -1.0;
        }
        return rate;
    }

    /**
     * Converts from former currency to euro (base), then from euro to requested
     * currency
     * 
     * @param value former currency value as double given by input
     * @param from  former currency given by input
     * @param to    requested currency given by input
     * @return the new converted value as a double
     */
    public static double convert(double value, String from, String to) {
        double rateFrom = pgFetch(from);
        double rateTo = pgFetch(to);
        return value / rateFrom * rateTo;
    }

    /**
     * Checks if an update is required based on the last update timestamps retrieved
     * from the database
     * 
     * @return true if an update is required, otherwise false
     */
    public static boolean isUpdateRequired() {
        try {
            // Query the database for the last update timestamps
            String sqlLastUpdated = "SELECT last_vacuum, last_autovacuum, last_analyze, last_autoanalyze " +
                    "FROM pg_stat_all_tables " +
                    "WHERE schemaname = 'rates' AND relname = 'rates_table'";
            PreparedStatement statement = jdbcConnect.prepareStatement(sqlLastUpdated);
            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                // Retrieve the last update timestamps from the result set
                java.sql.Timestamp lastVacuum = resultSet.getTimestamp("last_vacuum");
                java.sql.Timestamp lastAutoVacuum = resultSet.getTimestamp("last_autovacuum");
                java.sql.Timestamp lastAnalyze = resultSet.getTimestamp("last_analyze");
                java.sql.Timestamp lastAutoAnalyze = resultSet.getTimestamp("last_autoanalyze");

                // Determine the most recent timestamp and check if an update is required
                java.sql.Timestamp lastUpdated = getLastTimestamp(lastVacuum, lastAutoVacuum, lastAnalyze,
                        lastAutoAnalyze);
                if (lastUpdated != null) {
                    long currentTime = System.currentTimeMillis();
                    long lastUpdate = lastUpdated.getTime();
                    long fifteenMins = 15 * 60 * 1000; // 15 Minutes in milliseconds

                    return (currentTime - lastUpdate) > fifteenMins;
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error checking last update timestamp", e);
            e.printStackTrace();
        }

        return true; // Assume an update is required if an error occurs
    }

    /**
     * Retrieves the most recent timestamp from a given array of timestamps
     * 
     * @param timestamps an array of timestamps
     * @return the most recent timestamp, or null
     */
    private static java.sql.Timestamp getLastTimestamp(java.sql.Timestamp... timestamps) {
        java.sql.Timestamp maxTimestamp = null;
        for (java.sql.Timestamp timestamp : timestamps) {
            if (timestamp != null && (maxTimestamp == null || timestamp.after(maxTimestamp))) {
                // Update maxTimestamp if the current timestamp is newer
                maxTimestamp = timestamp;
            }
        }
        return maxTimestamp;
    }

    /**
     * Updates the rates in the schema by fetching data from the exchangerate.host
     * API, or inserts if missing
     */
    public static void updateRates() {
        apiConnect();
        try {
            JsonParser jp = new JsonParser();
            JsonElement root = jp.parse(new InputStreamReader((InputStream) apiConnect.getContent()));
            JsonObject rootobj = root.getAsJsonObject();
            JsonObject rates = rootobj.getAsJsonObject("rates");

            String sqlInsert = "INSERT INTO rates.rates_table (id, name, rates) VALUES (DEFAULT, ?, ?)";
            String sqlUpdate = "UPDATE rates.rates_table SET rates = ? WHERE name = ?";
            String sqlCheckExistence = "SELECT COUNT(*) FROM rates.rates_table WHERE name = ?";
            PreparedStatement insertStatement = jdbcConnect.prepareStatement(sqlInsert);
            PreparedStatement updateStatement = jdbcConnect.prepareStatement(sqlUpdate);
            PreparedStatement checkExistenceStatement = jdbcConnect.prepareStatement(sqlCheckExistence);

            Set<Map.Entry<String, JsonElement>> entries = rates.entrySet();
            for (Map.Entry<String, JsonElement> entry : entries) {
                String currency = entry.getKey();
                double rate = entry.getValue().getAsDouble();
                checkExistenceStatement.setString(1, currency);
                ResultSet resultSet = checkExistenceStatement.executeQuery();
                resultSet.next();
                int count = resultSet.getInt(1);
                if (count > 0) {
                    // Update the existing rates if there already exists rates
                    updateStatement.setDouble(1, rate);
                    updateStatement.setString(2, currency);
                    updateStatement.executeUpdate();
                } else {
                    // Insert a new record if no rates exist
                    insertStatement.setString(1, currency);
                    insertStatement.setDouble(2, rate);
                    insertStatement.executeUpdate();
                }
            }
            logger.info("Rates updated");
        } catch (SQLException | IOException e) {
            logger.log(Level.SEVERE, "Error updating rates", e);
            e.printStackTrace();
        }
        apiDisconnect();
    }

}
