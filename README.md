## Expedia fork
This is a fork to facilitate some changes needed for a different project called https://github.com/ExpediaGroup/waggle-dance. There is no intention at this time to make any other changes except to facilitate Waggle Dance. Please make any contributions to the upstream project if needed.

## AWS Glue Data Catalog Client for Apache Hive Metastore
The AWS Glue Data Catalog is a fully managed, Apache Hive Metastore compatible, metadata repository. Customers can use the Data Catalog as a central repository to store structural and operational metadata for their data.

AWS Glue provides out-of-box integration with Amazon EMR that enables customers to use the AWS Glue Data Catalog as an external Hive Metastore. To learn more, visit our [documentation](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive-metastore-glue.html).

This is an open-source implementation of the Apache Hive Metastore client on Amazon EMR clusters that uses the AWS Glue Data Catalog as an external Hive Metastore. It serves as a reference implementation for building a Hive Metastore-compatible client that connects to the AWS Glue Data Catalog. It may be ported to other Hive Metastore-compatible platforms such as other Hadoop and Apache Spark distributions.

This package is compatible with Spark 3 and Hive 3.

**Note**: in order for this client implementation to be used with Apache Hive, a patch included in this [JIRA](https://issues.apache.org/jira/browse/HIVE-12679) must be applied to it. All versions of Apache Hive running on Amazon EMR that support the AWS Glue Data Catalog as the metastore already include this patch. Please follow **all steps listed below in the following order**.

## Patching Apache Hive and Installing It Locally

Obtain a copy of Hive from GitHub at https://github.com/apache/hive.

	git clone https://github.com/apache/hive.git

To build the Hive client, you need to first apply this [patch](https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore/blob/branch-3.4.0/branch_3.1.patch).  Download this patch and move it to your local Hive git repository you created above. This patch is included in the repository. Apply the patch and build Hive.

    cd <your local Hive repo>	
    git checkout branch-3.1
	git apply -3 branch_3.1.patch
	mvn clean install -DskipTests

As Spark uses a fork of Hive based off the 2.3 branch, in order to build the Spark client, you need Hive 2.3 built with this [patch](https://issues.apache.org/jira/secure/attachment/12958418/HIVE-12679.branch-2.3.patch).

If building off the previous Hive repo, please reset those changes:

    git add .
    git reset --hard

Continue with patching the 2.3 branch:

	cd <your local Hive repo>
	git checkout branch-2.3
    patch -p0 <HIVE-12679.branch-2.3.patch
    mvn clean install -DskipTests

## Building the Glue Data Catalog Client

When building for the first time, all clients must be built from the root directory of the AWS Glue Data Catalog Client repository. This will build both the Hive and Spark clients and necessary dependencies.

    cd aws-glue-data-catalog-client-for-apache-hive-metastore
    mvn clean install -DskipTests

Once this is done, the individual clients are free to be built separately:

To build the Spark client:

    cd aws-glue-datacatalog-spark-client
    mvn clean package -DskipTests

To build the Hive client:

    cd aws-glue-datacatalog-hive3-client
    mvn clean package -DskipTests

## Configuring Hive to Use the Hive Client

You need to ensure that the AWS Glue Data Catalog Client jar is in Hive's CLASSPATH and also set the "hive.metastore.client.factory.class" HiveConf variable for Hive to pick up and instantiate the AWS Glue Data Catalog Client.  For instance, on Amazon EMR, the client jar is located in /usr/lib/hive/lib/ and the HiveConf is set in /usr/lib/hive/conf/hive-site.xml.

	<property>
 		<name>hive.metastore.client.factory.class</name>
 		<value>com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory</value>
	</property>

## Configuring Spark to Use the Spark Client

Similarly, for Spark, you need to install the client jar in Spark's CLASSPATH and create or update Spark's own hive-site.xml to add the above property.  On Amazon EMR, this is set in /usr/lib/spark/conf/hive-site.xml.  You can also find the location of the Spark client jar in /usr/lib/spark/conf/spark-defaults.conf.

## Enabling client side caching for catalog

Currently, we provide support for caching:

a) Table metadata - Response from Glue's GetTable operation (https://docs.aws.amazon.com/glue/latest/webapi/API_GetTable.html#API_GetTable_ResponseSyntax)
b) Database metadata - Response from Glue's GetDatabase operation (https://docs.aws.amazon.com/glue/latest/webapi/API_GetDatabase.html#API_GetDatabase_ResponseSyntax)

Both these entities have dedicated caches for themselves and can be enabled/tuned individually.

To enable/tune Table cache, use the following properties in your hive/spark configuration file:

	<property>
 		<name>aws.glue.cache.table.enable</name>
 		<value>true</value>
	</property>
	<property>
 		<name>aws.glue.cache.table.size</name>
 		<value>1000</value>
	</property>
	<property>
 		<name>aws.glue.cache.table.ttl-mins</name>
 		<value>30</value>
	</property>

To enable/tune Database cache:

	<property>
 		<name>aws.glue.cache.db.enable</name>
 		<value>true</value>
	</property>
	<property>
 		<name>aws.glue.cache.db.size</name>
 		<value>1000</value>
	</property>
	<property>
 		<name>aws.glue.cache.db.ttl-mins</name>
 		<value>30</value>
	</property>

NOTE: The caching logic is disabled by default.

## Sending drop events to metastore listeners (Expedia fork)

A Hive metastore calls the listeners in `hive.metastore.event.listeners` while it still holds a dropped table, so
their events carry the table's location and parameters. Glue keeps no copy of a dropped table, and its own events
carry names only. This client can call Hive `MetaStoreEventListener`s itself after it drops a table or partition,
using the table and partitions it read before the delete. Tools that clean up dropped data, such as Beekeeper
through Apiary's `ApiarySnsListener`, then get the same events as from a Hive metastore.

	<property>
		<name>aws.glue.event.listeners</name>
		<value>com.expediagroup.apiary.extensions.events.metastore.listener.ApiarySnsListener</value>
	</property>

- Only `onDropTable` and `onDropPartition` are called, after Glue confirms the delete.
- Every event carries the table as it is at the time of the drop, as from a Hive metastore. For a partition drop,
  the table is read on the drop call, right after Glue confirms the delete: one extra Glue read per partition drop
  call (one per batch for `dropPartitions` by expression), only when listeners are configured. If it can't be read,
  that drop's events are skipped and logged; the drop itself still succeeds.
- Listeners run on one background daemon thread with a queue of 10,000 drops, so they never slow down or fail a
  drop. When the queue is full, the drop's events are skipped and logged. On JVM shutdown, queued events are sent
  first, waiting up to 10 seconds; any still queued after that are logged.
- Dropping a table sends one drop table event; its partitions are deleted without events of their own.
- Each listener needs a public constructor taking a Hadoop `Configuration`, and is loaded through the thread context
  classloader. If a listener can't be loaded, the error is logged and no events are sent; the client still works.
- Listeners are created once per JVM for each value of the setting, with the configuration of the first client that
  names them, and shared by every client after that. Hosts such as Waggle Dance create a client for every metastore
  status check, and a listener like `ApiarySnsListener` holds an SNS client that is never closed.
- Hive 3 client only.

The setting is empty by default, which sends no events.

Two optional settings limit which tables send events. Both apply to table and partition drops alike, and both are
off by default:

	<property>
		<name>aws.glue.event.listeners.required-table-parameter</name>
		<value>beekeeper.remove.unreferenced.data</value>
	</property>
	<property>
		<name>aws.glue.event.listeners.skip-iceberg</name>
		<value>true</value>
	</property>

- `aws.glue.event.listeners.required-table-parameter`: only tables where this parameter is `true` (case-insensitive)
  send events. For Beekeeper, `beekeeper.remove.unreferenced.data` is the parameter its drop handling depends on.
- `aws.glue.event.listeners.skip-iceberg`: no events for Iceberg tables, i.e. tables with a `metadata_location`
  parameter or a `table_type` containing `iceberg`, the same test Beekeeper uses. This keeps a mistakenly tagged
  Iceberg table from ever reaching a cleanup tool.

When the host has Micrometer on its classpath, the client reports metrics to Micrometer's global registry; Waggle
Dance, through Spring Boot, adds its registries (such as Prometheus) to it. Without Micrometer, no metrics are
recorded and nothing else changes. Micrometer is a `provided` dependency, so it is not shipped with this client.

| Meter | Type | Tags | Meaning |
|-------|------|------|---------|
| `glue.drop.events` | counter | `type` (`table`, `partition`), `result` (`delivered`, `failed`, `skipped`, `filtered`) | `delivered` and `failed` count listener calls; `skipped` (queue full or shutting down) and `filtered` (table filters) count events |
| `glue.drop.events.listener` | timer | `type` | Time of one listener call, e.g. one SNS publish for `ApiarySnsListener` |
| `glue.drop.events.table.read` | timer | `result` (`success`, `failure`) | Time to read a dropped partition's table on the drop call: what events add to a partition drop |
| `glue.drop.events.queue.size` | gauge | | Events waiting to be sent |

`ApiarySnsListener` takes its own settings from environment variables, not from this configuration: `SNS_ARN`
(the topic to publish to) and `TABLE_PARAM_FILTER` (a regex of the table parameters to include in the message).
For Beekeeper, set `TABLE_PARAM_FILTER=beekeeper.*|table_type|metadata_location`. The filter must match the whole
parameter name. Beekeeper needs the `beekeeper.*` parameters to act on an event at all, and `table_type` and
`metadata_location` to recognise and skip Iceberg tables. Without the filter, the messages carry no table parameters
and Beekeeper ignores them.

## License

This library is licensed under the Apache 2.0 License.
