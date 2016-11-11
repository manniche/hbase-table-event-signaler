package com.nzcorp.hbaseSecondaryIndexer;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;

//remember to add the hbase dependencies to the pom file
public class secondIndexProtein extends BaseRegionObserver {

    private HTablePool pool = null;

    private String destinationTable;

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

        pool = new HTablePool(env.getConfiguration(), 10); //create a connection pool to the hbase table
        destinationTable = env.getConfiguration().get("destination_table", "sequence"); // we default to the sequence table
    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> observerContext,
                        final Put put,
                        final WALEdit edit,
                        final Durability durability_enum)
            throws IOException
    {
        try {
            final List<Cell> list_of_cells = put.get(Bytes.toBytes("data"), Bytes.toBytes("sequence_hashkey"));

            if (list_of_cells.isEmpty()) {
                return;
            }

            //there should only be one hit for 'data:sequence_hashkey'
            Cell sequence_hash = list_of_cells.get(0);

            // get table object
            HTableInterface table = pool.getTable(Bytes.toBytes(destinationTable));

            // create index row key
            byte[] index_key = CellUtil.cloneValue(sequence_hash);
            byte[] index_value = put.getRow();

            Put indexput = new Put(index_key);
            indexput.add(Bytes.toBytes("data"), Bytes.toBytes("reference"), index_value);

            table.put(indexput);
            table.close();

        } catch (IllegalArgumentException ex) {
            // handle exception.?? how to handle exception?
        }

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        pool.close();
    }
}
