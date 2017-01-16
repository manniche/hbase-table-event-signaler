package net.nzcorp.hbase.mq_data_rippler;

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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * Writes a specified key to a secondary index in order to support join-like operations
 *
 * On the wiki page http://gitlab.nzcorp.net/bts/bbd/blob/develop/wiki/components/hbase-table-layout.md it is
 * specified that the secondary indexer on contig should be using assembly_accession_number.
 *
 * The logic in the secondary indexer is as follows (taking the si on contig as an example):
 *  1. receive Put on contig
 *  2. check that the put in on the column family e and the column qualifier assembly_accession_number
 *  3. if this is the case, insert or update the rowKey specified by assembly_accession_number on assembly_index
 *     with the value c:{the contig row key}
 *
 * After this update, the assembly_index will have a structure looking like
 *
 * row key 	| column family c |        |        | column family f | ...
 * ---------+-----------------+--------+--------+-----------------+-------
 * EFB1     | c:EFN1          | c:EFN2 | c:EFN3 | f:EFP1          | ...
 * ... 	    | ...             | ...    | ...    | ...             | ...
 *
 * This will let clients look up all contigs, features or proteins that have a specific assembly accession number. \
 * Similarly, the contig_index will allow clients to look up all features and proteins that have a specific
 * contig accession number.
 *
 *
 */
@SuppressWarnings("unused")
public class MQDataRippler extends BaseRegionObserver {

    private Connection conn;
    private String destinationTable;
    private String secCF;
    private String sourceColumn;
    private static final Log LOGGER = LogFactory.getLog(net.nzcorp.hbase.mq_data_rippler.MQDataRippler.class);


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

            if(new String(sourceKey).contains(":")) {
                sourceKey = new String(sourceKey).replace(':', '_').getBytes();
            }

            if(sourceValue.length == 0) {
                LOGGER.error(String.format("The key to write an entry in %s was empty! It came from %s={%s:%s}", secTable, new String(sourceKey),"e", sourceColumn));
            } else {
                LOGGER.info( String.format( "Upserting key %s with column value %s:%s", new String(sourceValue), secCF, new String( sourceKey ) ));
                /* Create new rowkey if none exists or append to the existing rowkey, possibly overwriting the values
                 * that previously were there
                 */
                Get sourceRowKey = new Get(sourceValue);
                if (secTable.exists(sourceRowKey)) {
                    Append append = new Append(sourceValue);
                    append.add(secCF.getBytes(), sourceKey, "".getBytes());
                    secTable.append(append);
                } else {
                    Put newVal = new Put(sourceValue);
                    newVal.addColumn(secCF.getBytes(), sourceKey, "".getBytes());
                    secTable.put(newVal);
                }
            }
            secTable.close();

        } catch (IllegalArgumentException ex) {
            LOGGER.fatal("During the postPut operation, something went horribly wrong", ex);
            //In order not to drop its marbles when the HBase server throws an IllegalArgumentException, we allow the
            // coprocessor to continue operating, thereby allowing the HBase RS to continue operating.
            // This exception is throws before the secondary index table is opened, so just move along
            return;

        }

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        conn.close();
    }
}
