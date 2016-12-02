#!/bin/bash

# cd src/hbase-coprocessors/secondary-index/hbase-secondary-indexer/
# mvn package
#for i in {1..5}; do scp target/hbase-secondary-indexer-0.0.2.jar sild0$i:/tmp/ && ssh -t sild0$i 'sudo cp /tmp/hbase-secondary-indexer-0.0.2.jar /usr/hdp/2.5.0.0-1245/hbase/lib/'; done
# ssh into sild01 and execute this file

if [ "$USER" -ne "hbase" ]
  then echo "Please run as hbase user"
  exit
fi

echo 'create "assembly_genome_index" | hbase shell'
echo 'create "dna_genome_index" | hbase shell'
echo 'create "dna_assembly_index" | hbase shell'
echo 'create "feature_genome_index" | hbase shell'
echo 'create "feature_assembly_index" | hbase shell'
echo 'create "feature_dna_index" | hbase shell'
echo 'create "protein_genome_index" | hbase shell'
echo 'create "protein_assembly_index" | hbase shell'
echo 'create "protein_dna_index" | hbase shell'
echo 'create "protein_feature_index" | hbase shell'

echo 'disable "assembly_genome_index" | hbase shell'
echo 'disable "dna_genome_index" | hbase shell'
echo 'disable "dna_assembly_index" | hbase shell'
echo 'disable "feature_genome_index" | hbase shell'
echo 'disable "feature_assembly_index" | hbase shell'
echo 'disable "feature_dna_index" | hbase shell'
echo 'disable "protein_genome_index" | hbase shell'
echo 'disable "protein_assembly_index" | hbase shell'
echo 'disable "protein_dna_index" | hbase shell'
echo 'disable "protein_feature_index" | hbase shell'

echo 'alter "assembly", METHOD => "table_att", "coprocessor"=>"/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-0.0.2.jar|com.nzcorp.hbase.secondary_indexer.SecondaryIndexWriter|5|destination_table=assembly_genome_index,source_column=genome_accession_number"| hbase shell'
# echo 'alter "dna", METHOD => "table_att", "coprocessor"=>"/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-0.0.2.jar|com.nzcorp.hbase.secondary_indexer.SecondaryIndexWriter|5|destination_table=dna_genome_index,source_column=genome_accession_number"| hbase shell'
# echo 'alter "dna", METHOD => "table_att", "coprocessor"=>"/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-0.0.2.jar|com.nzcorp.hbase.secondary_indexer.SecondaryIndexWriter|5|destination_table=dna_assembly_index,source_column=genome_accession_number"| hbase shell'
# echo 'alter "feature", METHOD => "table_att", "coprocessor"=>"/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-0.0.2.jar|com.nzcorp.hbase.secondary_indexer.SecondaryIndexWriter|5|destination_table=feature_genome_index,source_column=genome_accession_number"| hbase shell'
# echo 'alter "feature", METHOD => "table_att", "coprocessor"=>"/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-0.0.2.jar|com.nzcorp.hbase.secondary_indexer.SecondaryIndexWriter|5|destination_table=feature_assembly_index,source_column=genome_accession_number"| hbase shell'
# echo 'alter "feature", METHOD => "table_att", "coprocessor"=>"/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-0.0.2.jar|com.nzcorp.hbase.secondary_indexer.SecondaryIndexWriter|5|destination_table=feature_dna_index,source_column=genome_accession_number"| hbase shell'
# echo 'alter "protein", METHOD => "table_att", "coprocessor"=>"/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-0.0.2.jar|com.nzcorp.hbase.secondary_indexer.SecondaryIndexWriter|5|destination_table=protein_genome_index,source_column=genome_accession_number"| hbase shell'
# echo 'alter "protein", METHOD => "table_att", "coprocessor"=>"/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-0.0.2.jar|com.nzcorp.hbase.secondary_indexer.SecondaryIndexWriter|5|destination_table=protein_assembly_index,source_column=genome_accession_number"| hbase shell'
# echo 'alter "protein", METHOD => "table_att", "coprocessor"=>"/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-0.0.2.jar|com.nzcorp.hbase.secondary_indexer.SecondaryIndexWriter|5|destination_table=protein_dna_index,source_column=genome_accession_number"| hbase shell'
# echo 'alter "protein", METHOD => "table_att", "coprocessor"=>"/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-0.0.2.jar|com.nzcorp.hbase.secondary_indexer.SecondaryIndexWriter|5|destination_table=protein_assembly_index,source_column=genome_accession_number"| hbase shell'

echo 'enable "assembly_genome_index" | hbase shell'
echo 'enable "dna_genome_index" | hbase shell'
echo 'enable "dna_assembly_index" | hbase shell'
echo 'enable "feature_genome_index" | hbase shell'
echo 'enable "feature_assembly_index" | hbase shell'
echo 'enable "feature_dna_index" | hbase shell'
echo 'enable "protein_genome_index" | hbase shell'
echo 'enable "protein_assembly_index" | hbase shell'
echo 'enable "protein_dna_index" | hbase shell'
echo 'enable "protein_feature_index" | hbase shell'

