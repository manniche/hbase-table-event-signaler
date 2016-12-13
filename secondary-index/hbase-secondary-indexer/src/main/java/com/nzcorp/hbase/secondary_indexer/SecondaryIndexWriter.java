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
    private String secCF;
    private String sourceColumn;
    private static final Log LOGGER = LogFactory.getLog(SecondaryIndexWriter.class);


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

        secCF = env.getConfiguration().get("secondary_idx_cf");
        sourceColumn = env.getConfiguration().get("source_column");
	    LOGGER.info("Initializing secondary indexer with destination table "+destinationTable+" and looking for "+sourceColumn);
        LOGGER.info("Will append to key in secondary index with column family "+secCF);
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
            Cell sourceColumnCell = cells.get(0);
            byte[] sourceValue = CellUtil.cloneValue(sourceColumnCell);

            Table secTable = conn.getTable(TableName.valueOf(destinationTable));

            byte[] sourceKey = put.getRow();

            LOGGER.debug( String.format( "Upserting key %s with value %s", sourceValue, secCF + new String( sourceKey ) ));
            /* Create new rowkey if none exists or append to the existing rowkey, possibly overwriting the values that
             * previously were there
             */

            Get tester = new Get( sourceValue );
            if( secTable.exists( tester ) )
            {
                Append append = new Append( sourceValue );
                append.add( secCF.getBytes(), sourceKey, "".getBytes() );
                secTable.append( append );
            } else
            {
                Put newVal = new Put( sourceValue );
                newVal.addColumn( secCF.getBytes(), sourceKey, "".getBytes() );
                secTable.put( newVal );
            }

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
