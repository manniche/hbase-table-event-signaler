To build you own (basically just document describing my PoC-steps):

- Install maven: `sudo apt-get install maven`
- Create new project for a coprocessor, e.g.:

```bash
mvn -B archetype:generate \
-DarchetypeGroupId=org.apache.maven.archetypes \
-DgroupId=com.nzcorp.hbase-secondary-index \
-DartifactId=hbase-secondary-indexer
```

- Rename it to the class name of the coprocessor
- Implement the coprocessor:
	- Resources:
		- https://community.hortonworks.com/articles/42946/creating-an-hbase-coprocessor-in-java.html
		- http://www.3pillarglobal.com/insights/hbase-coprocessors
		- http://saurzcode.in/2015/01/write-coprocessor-hbase/ 
		- http://stackoverflow.com/questions/14540167/create-secondary-index-using-coprocesor-hbase

- Remember to consult the java-docs for the **relevant** version of HBase. The best link I've found:
	- https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.4.2/bk_hbase_java_api/index.html
	- APIs get depricated all the time and a lot has changed from `0.98` to `1.x.x`!

- Remember to add all the dependecies as imports

- Edit the `pom.xml` to include relevant dependencies, e.g `hbase-server`

- Build the coprocessor with `mvn package`
	- handle and fix any build errors that might occur

- Write unit tests if relevant

- Test with the docker set-up:
	- Move the `*.jar` in `target` to the `/bbd/docker/hbase/lib`-folder
	- Load the coprocessor for a table in the shell:
		- `disable 'protein'`
		- `create 'sequence', 'data'`
		- `alter 'protein', 'coprocessor'=>'/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-{version number}.jar | com.nzcorp.hbaseSecondaryIndexer.secondIndexProtein | 5'`
		- `enable 'protein'`
		- `put 'protein', 'test_key', 'e:sequence_hashkey', 'md5hash'`
		- `scan 'sequence'`

# Install the coprocessor on a table

```
% cd bbd/src/hbase-coprocessors/secondary-index/hbase-secondary-index
% mvn package
% ssh {one of the cluster nodes}
% sudo -u hbase hbase shell

hbase(main):001:0> alter '{table_name}', METHOD => 'table_att', 'coprocessor'=>'hdfs:///user/hbase/hbase-secondary-indexer-{version number}.jar|com.nzcorp.hbaseSecondaryIndexer.SecondaryIndexWriter|5|destination_table={where to write secondary index},source_key={the source key},source_column={the column to look for the source key in}'
```

For the assembly table, the `destination_table` will be
genome_assembly_index and `source_key1` and `source_key2` are the
constituents of the value to be written in the secondary index.


For testing the secondary indexer in the docker setup, build and copy the secondary indexer to the docker libs:

```
cd src/hbase-coprocessors/secondary-index/hbase-secondary-indexer
mvn package
cp target/hbase-secondary-indexer-{version number}.jar ../../../../docker/hbase/lib/hbase-secondary-indexer-{version number}.jar
```

And after that, execute the following in the docker container (`docker exec -it /bin/bash` and `hbase shell`):

```
create 'assembly', 'e'
create 'assembly_genome_index', 'a', 'n', 'p'
disable 'assembly'
alter 'assembly', METHOD => 'table_att', 'coprocessor'=>'/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-{correct version string}.jar|com.nzcorp.hbase.secondary_indexer.SecondaryIndexWriter|5|destination_table=assembly_genome_index,source_column=genome_accession_number,secondary_idx_cf=a'
enable 'assembly'
put 'assembly', 'EFB1', 'e:genome_accession_number', 'EFG1'
scan 'genome_assembly_index', LIMIT=>1
```

Install the data rippler in the docker container with:

```
create 'assembly', 'e', 'eg'
create 'assembly_genome_index', 'd'
put 'assembly_genome_index', 'EFG1+EFB1', 'd', ''
create 'genome', 'e'
disable 'genome'
alter 'genome', METHOD => 'table_att', 'coprocessor'=>'/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-data-rippler-{correct version string}.jar|com.nzcorp.hbase.data_rippler.DownstreamDataRippler|10|destination_table=assembly,secondary_index_table=assembly_genome_index,source_column_family=e,target_column_family=eg'
enable 'genome'
put 'genome', 'EFG1', 'e:accession_number', 'EFG1'
```


On a running instance on the production cluster, upload the jar files to hdfs with

```
sudo -u hbase hdfs dfs -put bbd/src/hbase-coprocessors/secondary-index/hbase-data-rippler/target/hbase-data-rippler-{version string}.jar
```

and then install the data rippler with:

```
alter 'assembly', 'eg'
disable 'genome'
alter 'genome', METHOD => 'table_att', 'coprocessor'=>'hdfs:///user/hbase/hbase-data-rippler-{correct version string}.jar|com.nzcorp.hbase.data_rippler.DownstreamDataRippler|20|destination_table=assembly,secondary_index_table=assembly_genome_index,source_column_family=e,target_column_family=eg'
enable 'genome'
```


Removing a coprocessor

```
disable 'genome'
alter 'genome', METHOD => 'table_att_unset', NAME=>'coprocessor$1'
enable 'genome'
```

Docker testing the interaction between the secondary indexer and the data rippler (set the correct version number for the jars respectively:

```
create 'assembly', 'e', 'eg'
create 'assembly_genome_index', 'a', 'n', 'p'
create 'genome', 'e'
disable 'assembly'
disble 'genome'
alter 'assembly', METHOD => 'table_att', 'coprocessor'=>'/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-0.2.1.jar|com.nzcorp.hbase.secondary_indexer.SecondaryIndexWriter|5|destination_table=assembly_genome_index,source_column=genome_accession_number,secondary_idx_cf=a'
alter 'genome', METHOD => 'table_att', 'coprocessor'=>'/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-data-rippler-0.2.1.jar|com.nzcorp.hbase.data_rippler.DownstreamDataRippler|10|destination_table=assembly,secondary_index_table=assembly_genome_index,index_column_family=a,source_column_family=e,target_column_family=eg'
enable 'assembly'
enable 'genome'
put 'assembly', 'EFB1', 'e:genome_accession_number', 'EFG1'
put 'assembly', 'EFB2', 'e:genome_accession_number', 'EFG1'
put 'genome', 'EFG1', 'e:accession_number', 'EFG1'

