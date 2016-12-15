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
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//remember to add the hbase dependencies to the pom file
@SuppressWarnings("unused")
public class DownstreamDataRippler extends BaseRegionObserver {

    private Connection conn;
    private String destinationTable;
    private String secondaryIndexColumnFamily;
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

        secondaryIndexColumnFamily = env.getConfiguration().get("index_column_family");

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

            //Cell cell = list_of_cells.get(0);
            LOGGER.info("Found "+Integer.toString(list_of_cells.size())+" cells in Put");

            for (Cell cell: list_of_cells) {
                final byte[] rowKey = CellUtil.cloneRow(cell);
                final byte[] family = targetCf.getBytes();
                final byte[] qualifier = CellUtil.cloneQualifier(cell);
                final byte[] value = CellUtil.cloneValue(cell);

                LOGGER.trace(String.format("Found rowkey: %s", new String(rowKey)));

                List<byte[]> targetRowkeys = getTargetRowkeys(rowKey, secondaryIndexColumnFamily);
                LOGGER.trace("Got " + Integer.toString(targetRowkeys.size()) + " assemblykeys from " + new String(rowKey) + " prefix");
                // get table object
                table = conn.getTable(TableName.valueOf(destinationTable));
                for (byte[] targetKey : targetRowkeys) {
                    LOGGER.trace("Put'ing into " + destinationTable + ": " + new String(targetKey));
                    Put targetData = new Put(targetKey).addColumn(family, qualifier, value);
                    LOGGER.trace(String.format("Will insert %s:%s = %s", new String(family), new String(qualifier), new String(value)));
                    table.put(targetData);
                }
            }

        } catch (IllegalArgumentException ex) {
            LOGGER.fatal("During the postPut operation, something went horribly wrong", ex);
            if( table != null ) {
                table.close();
            }
            throw new IllegalArgumentException(ex);
        }

    }

    private List<byte[]> getTargetRowkeys(byte[] rowKey, String secondaryIndexColumnFamily) throws IOException{
        List<byte[]> targetKeys = new ArrayList<byte[]>();
        Table secTable = conn.getTable(TableName.valueOf(secondaryIndexTable));

        Get getter = new Get(rowKey);
        Result result = secTable.get(getter);
        List<Cell> cellList = result.listCells();

        for( Cell ccell: cellList )
        {
            if( new String( CellUtil.cloneFamily( ccell ) ).equals( secondaryIndexColumnFamily ) ){
                LOGGER.info(String.format("got column %s", new String(CellUtil.cloneQualifier(ccell))));
                targetKeys.add(CellUtil.cloneQualifier(ccell));
            }
        }
        secTable.close();
        return targetKeys;
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        conn.close();
    }
}
