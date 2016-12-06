package com.nzcorp.hbase.data_rippler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//remember to add the hbase dependencies to the pom file
@SuppressWarnings("unused")
public class DownstreamDataRippler extends BaseRegionObserver {

    private Connection conn;
    private String destinationTable;
    private String secondaryIndexTable;
    private String sourceCf;
    private String targetCf;
    private static final Log LOGGER = LogFactory.getLog(DownstreamDataRippler.class);


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

        conn = ConnectionFactory.createConnection( env.getConfiguration() );

        destinationTable = env.getConfiguration().get("destination_table");
        LOGGER.info( String.format( "Using destination table %s", destinationTable ));
        try {
            conn.getTable(TableName.valueOf(destinationTable));
        } catch ( IOException ioe ){
            String err = "Table "+destinationTable+" does not exist";
            LOGGER.error(err, ioe);
            throw new IOException( err, ioe);
        }

        // the column family name to take all values from
        secondaryIndexTable = env.getConfiguration().get("secondary_index_table");

        // the column family name to take all values from
        sourceCf = env.getConfiguration().get("source_column_family");

        // the column family name to put values into in the destinationTable
        targetCf = env.getConfiguration().get("target_column_family");

        LOGGER.info("Initializing data rippler copying values from column family "+sourceCf+" to "+destinationTable+":"+targetCf );
        LOGGER.info("Using secondary index "+secondaryIndexTable);

    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> observerContext,
                        final Put put,
                        final WALEdit edit,
                        final Durability durability_enum)
            throws IOException
    {
	Table table = null;
        try {
	    final ArrayList<Cell> list_of_cells = edit.getCells();
	    
            if (list_of_cells.isEmpty()) {
                return;
            }

            Table secTable = conn.getTable(TableName.valueOf(secondaryIndexTable));
            //Cell cell = list_of_cells.get(0);
            LOGGER.info("Found "+Integer.toString(list_of_cells.size())+" cells in Put");

            for (Cell cell: list_of_cells) {
                final byte[] rowKey = CellUtil.cloneRow(cell);
                final byte[] family = targetCf.getBytes();
                final byte[] qualifier = CellUtil.cloneQualifier(cell);
                final byte[] value = CellUtil.cloneValue(cell);

                LOGGER.trace(String.format("Found rowkey: %s", new String(rowKey)));

                Scan scan = new Scan();
                scan.setFilter(new PrefixFilter(rowKey));
                ResultScanner resultScanner = secTable.getScanner(scan);
                List<String> assemblyKeys = getTargetRowkeys(resultScanner);

                LOGGER.trace("Got " + Integer.toString(assemblyKeys.size()) + " assemblykeys from " + new String(rowKey) + " prefix");
                // get table object
                table = conn.getTable(TableName.valueOf(destinationTable));
                for (String assemblyKey : assemblyKeys) {
                    LOGGER.trace("Put'ing into " + destinationTable + ": " + assemblyKey);
                    Put targetData = new Put(Bytes.toBytes(assemblyKey)).addColumn(family, qualifier, value);
                    LOGGER.trace(String.format("Will insert %s:%s = %s", new String(family), new String(qualifier), new String(value)));
                    table.put(targetData);
                }
            }
            table.close();

        } catch (IllegalArgumentException ex) {
            LOGGER.fatal("During the postPut operation, something went horribly wrong", ex);
	    table.close();
            throw new IllegalArgumentException(ex);
        }

    }

    private List<String> getTargetRowkeys(ResultScanner resultScanner) {
        List<String> assemblyKeys = new ArrayList<String>();
        // get assembly rowkey from the secondary index
        for (Result result : resultScanner) {
            byte[] indexKey = result.getRow();
            LOGGER.trace(String.format("indexKey: %s", new String(indexKey)));
            String[] bits = new String(indexKey).split("\\+");
            LOGGER.debug(String.format("assemblyKey %s", bits[bits.length - 1]));
            assemblyKeys.add( bits[bits.length - 1] );
        }
        return assemblyKeys;
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        conn.close();
    }
}
