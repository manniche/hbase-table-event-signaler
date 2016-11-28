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
import java.util.Iterator;
import java.util.List;

//remember to add the hbase dependencies to the pom file
@SuppressWarnings("unused")
public class DownstreamDataRippler extends BaseRegionObserver {

    private Connection conn;
    private String destinationTable;
    private String targetCf;
    private String sourceCf;
    private String sourceTable;
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

        // TODO: we should check whether this table exists and abort if it does not
        destinationTable = env.getConfiguration().get("destination_table");
        try {
            conn.getTable(TableName.valueOf(destinationTable));
        } catch ( IOException ioe ){
            String err = "Table "+destinationTable+" does not exist";
            LOGGER.error(err, ioe);
            throw new IOException( err, ioe);
        }

        sourceTable = env.getConfiguration().get("source_table");
        try {
            conn.getTable(TableName.valueOf(sourceTable));
        } catch ( IOException ioe ){
            String err = "Table "+sourceTable+" does not exist";
            LOGGER.error(err, ioe);
            throw new IOException( err, ioe);
        }


        // the column family name to take all values from
        sourceCf = env.getConfiguration().get("source_column_family");

        // the column family name to put values into in the destinationTable
        targetCf = env.getConfiguration().get("target_column_family");

    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> observerContext,
                        final Put put,
                        final WALEdit edit,
                        final Durability durability_enum)
            throws IOException
    {
        try {
            final List<Cell> list_of_cells = put.get(Bytes.toBytes(sourceCf), Bytes.toBytes("*"));

            if (list_of_cells.isEmpty()) {
                return;
            }

            Table secTable = conn.getTable(TableName.valueOf(sourceTable+"_"+destinationTable+"_index"));

            // get table object
            Table table = conn.getTable(TableName.valueOf(destinationTable));

            for (Cell cell : list_of_cells) {
                byte[] rowKey = CellUtil.cloneRow(cell);
                byte[] family = Bytes.toBytes(targetCf);
                byte[] qualifier = cell.getQualifierArray();
                byte[] value = cell.getValueArray();

                Scan scan = new Scan();
                scan.setFilter(new PrefixFilter(rowKey));
                ResultScanner resultScanner = secTable.getScanner(scan);
                String assemblyKey = getTargetRowkey(resultScanner);

                Put targetData = new Put(Bytes.toBytes(assemblyKey));
                put.addColumn(family, qualifier, value);

                table.put(targetData);
            }

            table.close();

        } catch (IllegalArgumentException ex) {
            LOGGER.fatal("During the postPut operation, something went horribly wrong", ex);
            throw new IllegalArgumentException(ex);
        }

    }

    private String getTargetRowkey(ResultScanner resultScanner) {
        String assemblyKey = null;
        // get assembly rowkey from the secondary index
        for (Result result : resultScanner) {
            Cell secIdxCell = result.current();
            byte[] rowArray = secIdxCell.getRowArray();
            String thisRow = Bytes.toString(rowArray);
            String[] bits = thisRow.split("\\+");
            assemblyKey = bits[bits.length - 1];
            break;
        }
        return assemblyKey;

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        conn.close();
    }
}
