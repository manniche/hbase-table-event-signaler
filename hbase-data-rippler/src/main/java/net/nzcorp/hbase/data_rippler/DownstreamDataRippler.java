package net.nzcorp.hbase.data_rippler;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import net.nzcorp.ampq.types.ContentType;
import net.nzcorp.ampq.types.DeliveryType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import org.json.*;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeoutException;

//remember to add the hbase dependencies to the pom file
@SuppressWarnings("unused")
public class DownstreamDataRippler extends BaseRegionObserver {
    private static final Log LOGGER = LogFactory.getLog(DownstreamDataRippler.class);
    /**
     * Connection to HBase
     */
    private Connection conn;
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
     * Use with caution
     */
    private boolean f_debug;

    private static final double NANOS_TO_SECS = 1000000000.0;
    private String amq_address;

    private com.rabbitmq.client.ConnectionFactory factory;
    private com.rabbitmq.client.Connection connection;
    private com.rabbitmq.client.Channel channel;
    private boolean build_cache;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        /**
         * The CoprocessorEnvironment.getConfiguration() will return a
         * Hadoop Configuration element as described here:
         * https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/conf/Configuration.html
         *
         * The named arguments given after the last pipe separator when installing the coprocessor will be available on
         * the above configuration element
         */

        conn = ConnectionFactory.createConnection(env.getConfiguration());
        Admin hbAdmin = conn.getAdmin();

                destinationTable = env.getConfiguration().get("destination_table");
                if(destinationTable == null|| destinationTable.isEmpty()) {
                    String err = "No value for 'destination_table' specified, aborting coprocessor";
                    LOGGER.fatal(err);
                    throw new IllegalArgumentException(err);
                }
                LOGGER.info(String.format("Using destination queue %s", destinationTable));


        secondaryIndexTable = env.getConfiguration().get("secondary_index_table");
        if(secondaryIndexTable == null|| secondaryIndexTable.isEmpty()) {
            String err = "No value for 'secondary_index_table' specified, aborting coprocessor";
            LOGGER.fatal(err);
            throw new IllegalArgumentException(err);
        }
        if(!hbAdmin.tableExists(TableName.valueOf(secondaryIndexTable))) {
            String err = "Table " + secondaryIndexTable + " does not exist";
            LOGGER.fatal(err);
            throw new IOException(err);
        }
        LOGGER.info(String.format("Using secondary index table %s", secondaryIndexTable));


        secondaryIndexCF = env.getConfiguration().get("secondary_index_cf");
        if(secondaryIndexCF == null|| secondaryIndexCF.isEmpty()) {
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

        amq_address = env.getConfiguration().get("amq_address");

        if(amq_address == null || amq_address.isEmpty() || !amq_address.contains(":")) {
            String err = String.format("amq_address incorrectly configured (%s), cannot set up coprocessor", amq_address);
            LOGGER.fatal(err);
            throw new IOException(err);
        }
        try {
            factory = new com.rabbitmq.client.ConnectionFactory();
            factory.setHost(amq_address);
            factory.setVirtualHost("default");
            factory.setUsername("guest");
            factory.setPassword("guest");
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare("", BuiltinExchangeType.DIRECT, true);
            channel.queueDeclare(destinationTable, true, false, false, null);
        } catch (TimeoutException toe) {
            LOGGER.error(String.format("Timeout while trying to connect to MQ@%s", amq_address));
            throw new CoprocessorException(toe.getMessage());
        }

        build_cache = false;
        LOGGER.info("Initializing data rippler copying values from column family " + sourceCF + " to " + destinationTable + ":" + targetCf);
        LOGGER.info("Using secondary index " + secondaryIndexTable + ", column family: "+ secondaryIndexCF);

    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> observerContext,
                        final Put put,
                        final WALEdit edit,
                        final Durability durability_enum)
            throws IOException {
        Table table = null;
        Table secTable = null;
        try {
            LOGGER.trace("Entering DDR#postPut");
            long startTime = System.nanoTime();
            double lapTime;

            final String sourceTable = observerContext.getEnvironment().getRegionInfo().getTable().getNameAsString();

            /**
             * The Mutation.getCellList is the method we need, as we want to get all qualifiers of a given column family
             * Unfortunately, the method is package-private, and we only have access to the subclass `Put` here, so we
             * access the method by using the Reflection API. Please note that this is a last resort tactic and that we
             * will try to argue with the HBase projects, that there is a use case for exposing this method in the
             * public API. As `Mutation::getCellList` is not in the public API, the method could disappear without
             * deprecation warnings (or warnings at all). It will be in 2.0.0, as far as we know.
             */
            final Method meth = Mutation.class.getDeclaredMethod("getCellList", byte[].class);
            meth.setAccessible(true);
            final List<Cell> list_of_cells = (List<Cell>) meth.invoke(put, sourceCF.getBytes());

            if (list_of_cells.isEmpty()) {
                LOGGER.info("No cells in this transaction");
                return;
            }
            LOGGER.debug("Found " + Integer.toString(list_of_cells.size()) + " cells in Put");


            final Map<String, List<byte[]>> keysCache = new HashMap<String, List<byte[]>>();


            if (f_debug) {
                for (Cell cell : list_of_cells) {
                    final byte[] rowKey = CellUtil.cloneRow(cell);
                    LOGGER.debug(String.format("Found rowkey: %s", new String(rowKey)));
                }
            }

            for (Cell cell : list_of_cells) {
                final byte[] rowKey = CellUtil.cloneRow(cell);

                if(build_cache) {
                    if (!keysCache.containsKey(new String(rowKey))) {
                        lapTime = (double) (System.nanoTime()) / NANOS_TO_SECS;

                        LOGGER.debug(String.format("building cache for %s", new String(rowKey)));
                        keysCache.put(new String(rowKey), getTargetRowkeys(rowKey, secTable));
                        lapTime = (System.nanoTime() / NANOS_TO_SECS) - lapTime;
                        LOGGER.debug(String.format("Built cache in %f seconds", lapTime));
                    }
                }

                AMQP.BasicProperties headers = new AMQP.BasicProperties.Builder().
                        contentType(ContentType.JSON).
                        priority(1).
                        deliveryMode(DeliveryType.PERSISTENT).build();
                String message = constructJsonObject(keysCache, cell, rowKey);


                channel.basicPublish(destinationTable,
                                     new String(rowKey),
                                     headers,
                                     message.getBytes());
            }

            long endTime = System.nanoTime();
            double elapsedTime = (double) (endTime - startTime) / NANOS_TO_SECS;
            LOGGER.info(String.format("Exiting postPut, took %f seconds from start", elapsedTime));

        } catch (IllegalArgumentException ex) {
            LOGGER.fatal("During the postPut operation, something went horribly wrong", ex);
            //In order not to drop its marbles when the HBase server throws an IllegalArgumentException, we allow the
            // coprocessor to continue operating, thereby allowing the HBase RS to continue operating.
            // This exception is throws before the secondary index table is opened, so just move along
            throw new CoprocessorException(ex.getMessage());

        } catch (NoSuchMethodException e) {
            LOGGER.error("In trying to acquire reference to the Mutation::getCellList, an error occurred", e);
        } catch (IllegalAccessException e) {
            LOGGER.error("In trying to assign the reference to the Mutation::getCellList to a variable, an error occurred", e);
        } catch (InvocationTargetException e) {
            LOGGER.error("In trying to invoke the Mutation::getCellList, an error occurred", e);
        } finally {
            /**
             * Clean up resources that may have been left open
             */
            if (table != null) {
                table.close();
            }
            if (secTable != null) {
                secTable.close();
            }
        }
    }

    private String constructJsonObject(Map<String, List<byte[]>> keysCache, Cell cell, byte[] rowKey) {
        /**
         * If build_cache == true, we retrieve target keys from the secondary index and send them along in the
         * json message
        */

        JSONObject jo = new JSONObject();

        List<String> ls = new ArrayList<>();
        if(build_cache) {
            for( byte[] ba : keysCache.get(new String(rowKey)) ){
                ls.add(new String(ba));
            }
        }

         jo.put("column_family", targetCf);
         jo.put("column_qualifier", new String(CellUtil.cloneQualifier(cell)));
         jo.put("column_value",new String(CellUtil.cloneValue(cell)));
         jo.put("secondary_index", secondaryIndexTable);
         jo.put("secondary_index_cf", secondaryIndexCF);
         jo.put("target_keys", ls);

        return jo.toString();
    }

    private List<byte[]> getTargetRowkeys(byte[] rowKey, Table secTable) throws IOException {

        List<byte[]> targetKeys = new ArrayList<byte[]>();
        Result result = secTable.get(new Get(rowKey));
        List<Cell> cellList = result.listCells();

	if( cellList != null && cellList.size() > 0 ) {
	    for( Cell cell: cellList ) {
		byte[] cf = CellUtil.cloneFamily(cell);
		if (Arrays.equals( cf, secondaryIndexCF.getBytes())) {
		    LOGGER.debug(String.format("got column %s", new String(CellUtil.cloneQualifier(cell))));
		    targetKeys.add(CellUtil.cloneQualifier(cell));
		}
	    }
	} else {
	    LOGGER.warn( String.format("Found no targetKeys for %s", new String(rowKey)));
	}
	    return targetKeys;
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        conn.close();
    }
}
