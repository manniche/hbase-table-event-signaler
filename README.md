# HBase Table Event Signaler

This is an implementation of a coprocessor designed to be attached to a HBase table.

For each action defined in the configuration, the coprocessor will send a message with and optional value payload to an [AMQP](https://en.wikipedia.org/wiki/Advanced_Message_Queuing_Protocol) server.

Configuration options available:

 - destination_table
 - secondary_index_table
 - secondary_index_cf
 - source_column_family
 - target_column_family
 - amq_address
 - send_value
 - use_ssl