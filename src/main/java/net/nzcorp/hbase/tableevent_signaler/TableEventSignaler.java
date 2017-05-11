package net.nzcorp.hbase.tableevent_signaler;
/**
 * Copyright 2017 NovoZymes A/S
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.rabbitmq.client.*;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import net.nzcorp.amqp.Types;
import net.nzcorp.hbase.coprocessor.HookAction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Time;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

//remember to add the hbase dependencies to the pom file
public class TableEventSignaler extends BaseRegionObserver {
    private static final Log LOGGER = LogFactory.getLog(TableEventSignaler.class);

    /**
     * Connection to AMQP
     */
    private volatile Connection amqpConn;

    /**
     * The table into which the values from the current table should be written into
     */
    private String destinationTable;

    /**
     * The table from which the child table rowkeys should be retrieved from
     */
    private String secondaryIndexTable;

    /**
     * The column family to find target keys for in the secondary index
     */
    private String secondaryIndexCF;

    /**
     * The column family name to use in the destination table
     */
    private String targetCf;

    /**
     * The column family name from which to collect values from
     */
    private String sourceCF;

    /**
     * Whether to write a ridiculously amount of logging information
     * Use with caution as it introduces loops in the code path
     */
    private boolean f_debug;

    /**
     * Toggle whether TES should send the cell value along the with the message to AMQP
     */
    private boolean sendValue;

    /**
     * Applies a filtering to which column qualifiers to emit events for
     */
    private Set<String> filterQualifiers;

    private volatile Map<TableName, Table> tableCache;

    private ConnectionFactory factory;

    @Override
    public void start(final CoprocessorEnvironment env) throws IOException {

        destinationTable = env.getConfiguration().get("destination_table");
        if (Strings.isNullOrEmpty(destinationTable)) {
            String err = "No value for 'destination_tables' specified, aborting coprocessor";
            LOGGER.fatal(err);
            throw new IllegalArgumentException(err);
        }
        LOGGER.info(String.format("destination table set to %s", destinationTable));

        secondaryIndexTable = env.getConfiguration().get("secondary_index_table");
        if (Strings.isNullOrEmpty(secondaryIndexTable)) {
            String err = "No value for 'secondary_index_table' specified, aborting coprocessor";
            LOGGER.fatal(err);
            throw new IllegalArgumentException(err);
        }
        try (final Table _table = env.getTable(TableName.valueOf(secondaryIndexTable))) {
        } catch (IOException e) {
            String err = "Table " + secondaryIndexTable + " does not exist";
            LOGGER.fatal(err);
            throw e;
        }
        LOGGER.info(String.format("Using secondary index table %s", secondaryIndexTable));

        secondaryIndexCF = env.getConfiguration().get("secondary_index_cf");
        if (Strings.isNullOrEmpty(secondaryIndexCF)) {
            String err = "No 'secondary_index_cf' specified, cannot continue. Please set secondary_index_cf=some_sensible_value for the coprocessor";
            LOGGER.fatal(err);
            throw new IllegalArgumentException(err);
        }

        // the column family name to take all values from
        sourceCF = env.getConfiguration().get("source_column_family");

        // the column family name to put values into in the destinationTable
        targetCf = env.getConfiguration().get("target_column_family");

        //option to run *expensive* debugging
        f_debug = Boolean.parseBoolean(env.getConfiguration().get("full_debug"));

        //light-weight messages? anything other than -i "tRuE" is false:
        sendValue = Boolean.parseBoolean(env.getConfiguration().get("send_value"));

        String fqs = env.getConfiguration().get("filter_qualifiers");
        if (fqs == null || fqs.length() == 0) {
            LOGGER.info("No filter qualifiers set, signaling on every event");
            filterQualifiers = new HashSet<>();
        } else {
            filterQualifiers = Sets.newHashSet(fqs.split("\\|"));
            LOGGER.info(String.format("%s filters loaded", filterQualifiers.size()));
        }

        /*
         * The fully qualified amqpConn string to the amqp server
         *
         * e.g.  amqp://guest:guest@rabbitmq:5672/hbase_events
         */
        final String amqpAddress = env.getConfiguration().get("amq_address");

        if (Strings.isNullOrEmpty(amqpAddress)) {
            String err = "missing value for parameter amqpAddress";
            LOGGER.fatal(err);
            throw new IOException(err);
        }

        factory = new ConnectionFactory();
        try {
            factory.setUri(amqpAddress);
        } catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
            throw new IOException(e);
        }

        ensureAmqpConnection();

        tableCache = new HashMap<>();
        LOGGER.info(String.format("Sending from %s#%s: --> %s#%s", secondaryIndexTable, sourceCF, destinationTable, targetCf));
    }

    private void ensureAmqpConnection() throws IOException {
        if (amqpConn != null && amqpConn.isOpen()) {
            return;
        }
        // Synchronize connection creation as we don't want multiple threads overwriting each other's connections.
        synchronized (this) {
            if (amqpConn != null && amqpConn.isOpen()) {
                return;
            }
            try {
                amqpConn = factory.newConnection();
            } catch (AuthenticationFailureException | SocketTimeoutException | TimeoutException e) {
                LOGGER.fatal("Failed to connect to AMQP server", e);
                throw new IOException(e);
            } catch (IOException e) {
                LOGGER.fatal("Failed to connect to AMQP server", e);
                throw e;
            }
        }
    }

    private static class RowKey {
        private final byte[] byteArray;

        RowKey(final Cell cell) {
            this.byteArray = CellUtil.cloneRow(cell);
        }

        byte[] getRowKey() {
            return byteArray;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RowKey rowKey = (RowKey) o;

            return Arrays.equals(byteArray, rowKey.byteArray);

        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(byteArray);
        }
    }

    private Map<RowKey, Boolean> getNewRows(final RegionCoprocessorEnvironment env, final TableName tableName, final List<Cell> cells) throws IOException {
        final Map<RowKey, Boolean> newRows = new HashMap<>();

        try {
            Table table = tableCache.containsKey(tableName) ? tableCache.get(tableName) : refreshCache(tableName, env);
            checkAndSetNewRows(tableName, cells, newRows, table);
        } catch (NotServingRegionException nsre) {
            LOGGER.error("Cached table seemed to be stale, requesting new run");
            tableCache.remove(tableName);
            Table table = refreshCache(tableName, env);
            checkAndSetNewRows(tableName, cells, newRows, table);
        }
        return newRows;
    }

    private void checkAndSetNewRows(final TableName tableName, final List<Cell> cells, final Map<RowKey, Boolean> newRows, Table table) throws IOException {
        for (final Cell cell : cells) {
            final RowKey rowKey = new RowKey(cell);
            if (newRows.containsKey(rowKey)) {
                continue;
            }
            final Get get = new Get(rowKey.getRowKey());
            try {
                newRows.put(rowKey, !table.exists(get));
            } catch (Exception e) {
                LOGGER.error(String.format("When trying use a cached table for %s, the code threw", tableName), e);
                throw e;
            }
        }
    }

    private synchronized Table refreshCache(TableName tableName, final RegionCoprocessorEnvironment env) throws IOException {
        if(tableCache.containsKey(tableName)){ return tableCache.get(tableName); }

        long a = System.nanoTime();
        LOGGER.info("Trying to obtain connection for " + tableName);
        Table newCacheVal = env.getTable(tableName);
        LOGGER.debug(String.format("Obtained table ref in %d ms", NANOSECONDS.toMillis(System.nanoTime() - a)));
        tableCache.put(tableName, newCacheVal);
        return newCacheVal;
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> observerContext,
                       final Put put,
                       final WALEdit edit,
                       final Durability durability_enum)
            throws IOException {
        LOGGER.debug("Entering TES#prePut");
        final long startTime = System.nanoTime();

        final TableName tableName = observerContext.getEnvironment().getRegionInfo().getTable();
        if (tableName == null) {
            return;
        }

        final List<Cell> cellList = put.getFamilyCellMap().get(sourceCF.getBytes());
        if (cellList == null) {
            return;
        }
        LOGGER.trace(String.format("Found %s cells in put in %d ms from start", cellList.size(), NANOSECONDS.toMillis(System.nanoTime()-startTime)));
        if (f_debug) {
            for (Cell cell : cellList) {
                final byte[] rowKey = CellUtil.cloneRow(cell);
                LOGGER.debug(String.format("Found rowkey: %s", Bytes.toString(rowKey)));
            }
        }

        final Map<RowKey, Boolean> newRows = getNewRows(observerContext.getEnvironment(), tableName, cellList);
        LOGGER.debug(String.format("getNewRows at %d ms from start", NANOSECONDS.toMillis(System.nanoTime()-startTime)));
        for (final Cell cell : cellList) {

            if (!filterQualifiers.isEmpty() && !filterQualifiers.contains(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                continue;
            }
            final RowKey rowKey = new RowKey(cell);
            boolean isNewRow = newRows.get(rowKey);

            final String action = isNewRow ? HookAction.PUT : HookAction.UPDATE;
            final AMQP.BasicProperties headers = constructBasicProperties(action);
            final String message = constructJsonObject(cell, rowKey.getRowKey());
            LOGGER.debug(String.format("constructed json at %d ms from start", NANOSECONDS.toMillis(System.nanoTime()-startTime)));

            final String queueName = tableName.getNameAsString();
            publishMessage(queueName, headers, message);
            LOGGER.debug(String.format("published msg at %d ms from start", NANOSECONDS.toMillis(System.nanoTime()-startTime)));
        }
        long endTime = System.nanoTime();
        long elapsedTime = NANOSECONDS.toMillis(endTime - startTime);

        LOGGER.debug(String.format("Exiting TES#prePut, took %d ms (%d ns)from start", elapsedTime, endTime-startTime));
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                          Delete delete,
                          WALEdit edit,
                          Durability durability) throws IOException {
        long startTime = System.nanoTime();
        LOGGER.debug("Entering TES#preDelete");


        final TableName tableName = e.getEnvironment().getRegionInfo().getTable();
        if (tableName == null) {
            return;
        }

        final List<Cell> cellList = delete.getFamilyCellMap().get(sourceCF.getBytes());
        if (cellList == null || cellList.isEmpty()) {
            return;
        }

        final List<Cell> realCellList = new ArrayList<>();
        for (final Cell cell : cellList) {
            if (CellUtil.cloneQualifier(cell).length == 0) {
                realCellList.add(cell);
            }
        }

        for (final Cell cell : realCellList) {
            final byte[] rowKey = CellUtil.cloneRow(cell);

            final AMQP.BasicProperties headers = constructBasicProperties(HookAction.DELETE);
            final String message = constructJsonObject(cell, rowKey);
            final String queueName = tableName.getNameAsString();

            publishMessage(queueName, headers, message);
        }


        long endTime = System.nanoTime();
        long elapsedTime = NANOSECONDS.toMillis(endTime - startTime);
        LOGGER.debug(String.format("Exiting TES#preDelete, took %d ms (%d ns)from start", elapsedTime, endTime-startTime));
    }

    private void publishMessage(String queueName, AMQP.BasicProperties headers, String message) throws IOException {
        long pmStart = System.nanoTime();
        LOGGER.trace("Getting channel");
        final Channel channel = getChannel();
        try {
            LOGGER.trace(String.format("Ensuring that queue: %s exists", queueName));
            ensureQueue(channel, queueName);
            LOGGER.debug(String.format("Ensured channel in %d ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-pmStart)));

            LOGGER.trace(String.format("Sending message to queue: %s", queueName));
            channel.basicPublish(
                    "",
                    queueName,
                    headers,
                    message.getBytes());

            // Channel seems to work. Use it again.
            LOGGER.debug(String.format("Sent message in %d ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-pmStart)));
            LOGGER.trace("Message sent, releasing channel");
            releaseChannel(channel);
            LOGGER.debug(String.format("Released channel in %d ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-pmStart)));
        } catch (Throwable t) {
            // There was an error on the channel, throw it away.
            try {
                channel.close();
            } catch (Exception e) {
            }
            LOGGER.error(String.format("Error sending message to channel: %s", queueName), t);
            throw t;
        }
    }

    private AMQP.BasicProperties constructBasicProperties(final String action) {
        Map<String, Object> customHeader = new HashMap<>();
        customHeader.put("action", action);
        return new AMQP.BasicProperties.Builder().
                contentType(Types.ContentType.JSON).
                priority(1).
                headers(customHeader).
                timestamp(new Date()).
                deliveryMode(Types.DeliveryType.PERSISTENT).build();
    }

    private String constructJsonObject(Cell cell, byte[] rowKey) throws UnsupportedEncodingException {
        final JSONObject jo = new JSONObject();

        jo.put("row_key", Bytes.toString(rowKey));
        jo.put("column_family", targetCf);
        jo.put("column_qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));
        jo.put("column_value", Bytes.toString(sendValue ? CellUtil.cloneValue(cell) : new byte[]{}));
        jo.put("secondary_index", secondaryIndexTable);
        jo.put("secondary_index_cf", secondaryIndexCF);
        jo.put("destination_table", destinationTable);

        return jo.toString();
    }

    private final ConcurrentHashMap<String, Boolean> queuesCreated = new ConcurrentHashMap<>();

    private void ensureQueue(final Channel channel, final String queueName) throws IOException {
        if (!queuesCreated.getOrDefault(queueName, false)) {
            channel.queueDeclare(queueName, true, false, false, null);
            queuesCreated.put(queueName, true);
        }
    }

    private final ConcurrentLinkedDeque<Channel> channels = new ConcurrentLinkedDeque<>();

    private Channel getChannel() throws IOException {
        ensureAmqpConnection();

        // See if we already have an opened channel.
        final Channel c = channels.pollFirst();
        if (c != null && c.isOpen()) {
            // We have it, and it appears to be working.
            return c;
        }

        // Too few channels, let's create a new one
        return amqpConn.createChannel();
    }

    private void releaseChannel(Channel c) {
        channels.push(c);
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // Channels will be closed when connection is closed.
        amqpConn.close();
    }
}
