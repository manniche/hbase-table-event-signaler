package com.nzcorp.hbase.secondary_indexer;

import java.io.IOException;
import java.util.List;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * Writes a specified key to a secondary index in order to support join-like operations
 */
@SuppressWarnings("unused")
public class SecondaryIndexWriter extends BaseRegionObserver {

    private Connection conn;
    private String destinationTable;
    private String sourceColumn;
    private static final Log LOGGER = LogFactory.getLog(BaseRegionObserver.class);


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

        try {
            conn.getTable(TableName.valueOf(destinationTable));
        } catch ( IOException ioe ){
            String err = "Table "+destinationTable+" does not exist";
            LOGGER.warn(err, ioe);
            throw new IOException( err, ioe);
        }

        sourceColumn = env.getConfiguration().get("source_column");
	    LOGGER.info("Initializing secondary indexer with destination table "+destinationTable+" and looking for "+sourceColumn);
    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> observerContext,
                        final Put put,
                        final WALEdit edit,
                        final Durability durability_enum)
            throws IOException
    {
        try {

	    final List<Cell> cells = put.get(Bytes.toBytes("e"), Bytes.toBytes(sourceColumn));
            if (cells.isEmpty()) {
                return;
            }
            LOGGER.info( "Got cell object matching "+sourceColumn );
            Cell sourceColumnCell = cells.get(0);
            byte[] sourceValue = CellUtil.cloneValue(sourceColumnCell);

            Table secTable = conn.getTable(TableName.valueOf(destinationTable));

            byte[] sourceKey = put.getRow();

            ByteBuffer bb = ByteBuffer.allocate(sourceValue.length+1+sourceKey.length);
            bb.put( sourceValue );
            bb.put( "+".getBytes() );
            bb.put( sourceKey );

            byte[] finalKey = bb.array();

            Put targetData = new Put(finalKey);
            targetData.addColumn("data".getBytes(), "".getBytes(), "".getBytes());
            secTable.put(targetData);

            secTable.close();

        } catch (IllegalArgumentException ex) {
            LOGGER.fatal("During the postPut operation, something went horribly wrong", ex);
            throw new IllegalArgumentException(ex);
        }

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        conn.close();
    }
}
