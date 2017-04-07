#!/bin/bash

set -eu

if [ "$(whoami)" != "hbase" ]
  then echo "Please run as hbase user"
  exit 1
fi

if [ -f /.dockerenv ]; then
    JARLIB_PATH=/usr/hdp/2.5.0.0-1245/hbase/lib/nzcorp-tableevent-signaler
else
    JARLIB_PATH=hdfs:///user/hbase/nzcorp-tableevent-signaler
fi



hbase shell <<EOF

alter_async "$TABLE", METHOD => "table_att", "coprocessor"=>"${JARLIB_PATH}-$TES_VERSION.jar|net.nzcorp.${SHADE_PACKAGE}.hbase.tableevent_signaler.TableEventSignaler|5|destination_table=${DEST},secondary_index_table=${TABLE}_index,secondary_index_cf=${SI_CF},source_column_family=${SOURCE_CF},target_column_family=${DEST_CF},amq_address=${AMQ},send_value=${SENDVAL}"
EOF



