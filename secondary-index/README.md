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
		- `alter 'protein', 'coprocessor'=>'/usr/hdp/2.5.0.0-1245/hbase/lib/hbase-secondary-indexer-0.0.1.jar | com.nzcorp.hbaseSecondaryIndexer.secondIndexProtein | 5'`
		- `enable 'protein'`
		- `put 'protein', 'test_key', 'data:sequence_hashkey', 'md5hash'`
		- `scan 'sequence'`
