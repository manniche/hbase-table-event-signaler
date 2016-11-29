package com.nzcorp.hbase.secondary_indexer;

import java.io.IOException;
import java.util.List;

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
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * Writes a specified key to a secondary index in order to support join-like operations
 */
@SuppressWarnings("unused")
public class SecondaryIndexWriter extends BaseRegionObserver {

    private Connection conn;
    private String destinationTable;
    private String sourceKey;
    private String targetKey;
    private String sourceTable;
    private String targetCF;
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

        destinationTable = env.getConfiguration().get("index_table");

        try {
            conn.getTable(TableName.valueOf(destinationTable));
        } catch ( IOException ioe ){
            String err = "Table "+destinationTable+" does not exist";
            LOGGER.warn(err, ioe);
            throw new IOException( err, ioe);
        }

        sourceKey1 = env.getConfiguration().get("source_key1");
        sourceKey2 = env.getConfiguration().get("source_key2");

        targetCF = env.getConfiguration().get("target_cf");
    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> observerContext,
                        final Put put,
                        final WALEdit edit,
                        final Durability durability_enum)
            throws IOException
    {
        try {
            final List<Cell> cells1 = put.get(Bytes.toBytes("e"), Bytes.toBytes(sourceKey1));
            if (cells1.isEmpty()) {
                return;
            }
            Cell sourceKey1Cell = cells1.get(0);
            byte[] sourceKey1Value = cell.getValueArray();

            cells2 = put.get(Bytes.toBytes("e"), Bytes.toBytes(sourceKey2));
            if (cells2.isEmpty()) {
                return;
            }
            Cell sourceKey2Cell = cells2.get(0);
            byte[] sourceKey2Value = cell.getValueArray();

            Table secTable = conn.getTable(TableName.valueOf(sourceTable+"_"+destinationTable+"_index"));

            String finalKey = Bytes.toString(sourceKey1Value)+"+"+Bytes.toString(sourceKey2Value);

            Put targetData = new Put(Bytes.toBytes(finalKey));
            table.put(targetData);

            table.close();

        } catch (IllegalArgumentException ex) {
            LOGGER.fatal("During the postPut operation, something went horribly wrong", ex);
            throw new IllegalArgumentException(ex);
        }

    }

    private String getTargetRowkey(ResultScanner resultScanner) {
        String assemblyKey = null;
        // get assembly rowkey from the secondary index
        for( Result result: resultScanner){
            Cell secIdxCell = result.current();
            byte[] rowArray = secIdxCell.getRowArray();
            String thisRow = Bytes.toString(rowArray);
            String[] bits = thisRow.split("\\+");
            assemblyKey = bits[bits.length-1];
            break;
        }
        return assemblyKey;

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        conn.close();
    }
}
