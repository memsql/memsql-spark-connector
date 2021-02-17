# SingleStore Spark Connector
## Version: 3.1.1 [![Continuous Integration](https://circleci.com/gh/memsql/singlestore-spark-connector/tree/master.svg?style=shield)](https://circleci.com/gh/memsql/singlestore-spark-connector) [![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

## Getting Started

You can find the latest version of the connector on Maven Central and
spark-packages.org.  The group is `com.singlestore` and the artifact is
`singlestore-spark-connector_2.11` for Spark 2 and `singlestore-spark-connector_2.12` for Spark 3.

* [Maven Central](https://search.maven.org/artifact/com.singlestore/singlestore-spark-connector_2.11)
* [Maven Central (Spark 3)](https://search.maven.org/artifact/com.singlestore/singlestore-spark-connector_2.12)
* [spark-packages.org](https://spark-packages.org/package/memsql/memsql-spark-connector)

You can add the connector to your Spark application using: spark-shell, pyspark, or spark-submit
```
$SPARK_HOME/bin/spark-shell --packages com.singlestore:singlestore-spark-connector_2.12:3.1.1-spark-3.0.0
```

We release three versions of the `singlestore-spark-connector`, one per Spark version.
An example version number is: `3.1.1-spark-3.0.0` which is the 3.1.1
version of the connector, compiled and tested against Spark 3.0.0. Make sure
you are using the most recent version of the connector.

In addition to adding the `singlestore-spark-connector`, you will also need to have the
MariaDB JDBC driver installed.  This library is tested against the following
MariaDB driver version:

```
"org.mariadb.jdbc" % "mariadb-java-client"  % "2.+"
```

Once you have everything installed, you're almost ready to run your first
queries against SingleStore!

## Configuration

The `singlestore-spark-connector` is configurable globally via Spark options and
locally when constructing a DataFrame.  The options are named the same, however
global options have the prefix `spark.datasource.singlestore.`.

| Option                    | Description
| -                         | -
| `ddlEndpoint`  (required) | Hostname or IP address of the SingleStore Master Aggregator in the format `host[:port]` (port is optional). Ex. `master-agg.foo.internal:3308` or `master-agg.foo.internal`
| `dmlEndpoints`            | Hostname or IP address of SingleStore Aggregator nodes to run queries against in the format `host[:port],host[:port],...` (port is optional, multiple hosts separated by comma). Ex. `child-agg:3308,child-agg2` (default: `ddlEndpoint`)
| `user`                    | SingleStore username (default: `root`)
| `password`                | SingleStore password (default: no password)
| `query`                   | The query to run (mutually exclusive with dbtable)
| `dbtable`                 | The table to query (mutually exclusive with query)
| `database`                | If set, all connections will default to using this database (default: empty)
| `disablePushdown`         | Disable SQL Pushdown when running queries (default: false)
| `enableParallelRead`      | Enable reading data in parallel for some query shapes (default: false)
| `overwriteBehavior`       | Specify the behavior during Overwrite; one of `dropAndCreate`, `truncate`, `merge` (default: `dropAndCreate`)
| `truncate`                | :warning: **Deprecated option, please use `overwriteBehavior` instead** Truncate instead of drop an existing table during Overwrite (default: false)
| `loadDataCompression`     | Compress data on load; one of (`GZip`, `LZ4`, `Skip`) (default: GZip)
| `loadDataFormat`          | Serialize data on load; one of (`Avro`, `CSV`) (default: CSV)
| `tableKey`                | Specify additional keys to add to tables created by the connector (See below for more details)
| `onDuplicateKeySQL`       | If this option is specified, and a row is to be inserted that would result in a duplicate value in a PRIMARY KEY or UNIQUE index, SingleStore will instead perform an UPDATE of the old row. See examples below
| `insertBatchSize`         | Size of the batch for row insertion (default: `10000`)
| `maxErrors`               | The maximum number of errors in a single `LOAD DATA` request. When this limit is reached, the load fails. If this property equals to `0`, no error limit exists (Default: `0`)

## Examples

Example of configuring the `singlestore-spark-connector` globally:
```scala
spark.conf.set("spark.datasource.singlestore.ddlEndpoint", "singlestore-master.cluster.internal")
spark.conf.set("spark.datasource.singlestore.dmlEndpoints", "singlestore-master.cluster.internal,singlestore-child-1.cluster.internal:3307")
spark.conf.set("spark.datasource.singlestore.user", "admin")
spark.conf.set("spark.datasource.singlestore.password", "s3cur3-pa$$word")
```

Example of configuring the `singlestore-spark-connector` using the read API:
```scala
val df = spark.read
    .format("singlestore")
    .option("ddlEndpoint", "singlestore-master.cluster.internal")
    .option("user", "admin")
    .load("foo")
```

Example of configuring the `singlestore-spark-connector` using an external table in Spark SQL:
```sql
CREATE TABLE bar USING singlestore OPTIONS ('ddlEndpoint'='singlestore-master.cluster.internal','dbtable'='foo.bar')
```

For Java/Python versions of some of these examples, visit the section ["Java & Python Example"](#java-python-example)

## Writing to SingleStore

The `singlestore-spark-connector` supports saving dataframes to SingleStore using the Spark write API. Here is a basic example of using this API:

```scala
df.write
    .format("singlestore")
    .option("loadDataCompression", "LZ4")
    .option("overwriteBehavior", "dropAndCreate")
    .mode(SaveMode.Overwrite)
    .save("foo.bar") // in format: database.table
```

If the target table ("foo" in the example above) does not exist in SingleStore the
`singlestore-spark-connector` will automatically attempt to create the table. If you
specify SaveMode.Overwrite, if the target table already exists, it will be
recreated or truncated before load. Specify `overwriteBehavior = truncate` to truncate rather
than re-create.

### Retrieving the number of written rows from taskMetrics

It is possible to add the listener and get the number of written rows.

```scala
spark.sparkContext.addSparkListener(new SparkListener() {
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    println("Task id: " + taskEnd.taskInfo.id.toString)
    println("Records written: " + taskEnd.taskMetrics.outputMetrics.recordsWritten.toString)
  }
})

df.write.format("singlestore").save("example")
```

### Specifying keys for tables created by the Spark Connector
When creating a table, the `singlestore-spark-connector` will read options prefixed
with `tableKey`. These options must be formatted in a specific way in order to
correctly specify the keys.

> :warning: The default table type is SingleStore Columnstore. If you want a RowStore table,
> you will need to specify a Primary Key using the tableKey option.

To explain we will refer to the following example:

```scala
df.write
    .format("singlestore")
    .option("tableKey.primary", "id")
    .option("tableKey.key.created_firstname", "created, firstName")
    .option("tableKey.unique", "username")
    .mode(SaveMode.Overwrite)
    .save("foo.bar") // in format: database.table
```

In this example, we are creating three keys:
1. A primary key on the `id` column
2. A regular key on the columns `created, firstname` with the key name `created_firstname`
3. A unique key on the `username` column

Note on (2): Any key can optionally specify a name, just put it after the key type.
Key names must be unique.

To change the default ColumnStore sort key you can specify it explicitly:
```scala
df.write
    .option("tableKey.columnstore", "id")
```

You can also customize the shard key like so:
```scala
df.write
    .option("tableKey.shard", "id, timestamp")
```

## Inserting rows into the table with ON DUPLICATE KEY UPDATE

When updating a rowstore table it is possible to insert rows with `ON DUPLICATE KEY UPDATE` option.
See [sql reference](https://docs.singlestore.com/latest/reference/sql-reference/data-manipulation-language-dml/insert/) for more details.

```scala
df.write
    .option("onDuplicateKeySQL", "age = age + 1")
    .option("insertBatchSize", 300)
    .mode(SaveMode.Append)
    .save("foo.bar")
```

As a result of the following query, all new rows will be appended without changes.
If the row with the same `PRIMARY KEY` or `UNIQUE` index already exists then the corresponding `age` value will be increased.

When you use ON DUPLICATE KEY UPDATE, all rows of the data frame are split into batches, and every insert query will contain no more than the specified `insertBatchSize` rows setting.

## Save Modes

Save operations can optionally take a SaveMode, that specifies how to handle existing data if present. 
It is important to realize that these save modes do not utilize any locking and are not atomic. 
Additionally, when performing an Overwrite, the data will be deleted before writing out the new data.

1. `SaveMode.Append` means that when saving a DataFrame to a data source, if data/table already exists,
contents of the DataFrame are expected to be appended to existing data.
2. `SaveMode.Overwrite` means that when saving a DataFrame to a data source,
if data/table already exists, existing data is expected to be overwritten by the contents of the DataFrame.    
> `Overwrite` mode depends on `overwriteBehavior` option, for better understanding look at the section ["Merging on save"](#merging-on-save)
3. `SaveMode.ErrorIfExists` means that when saving a DataFrame to a data source, 
if data already exists, an exception is expected to be thrown.
4. `SaveMode.Ignore` means that when saving a DataFrame to a data source, if data already exists,
the save operation is expected to not save the contents of the DataFrame and to not change the existing data.

### Example of `SaveMode` option

```scala
df.write
    .mode(SaveMode.Append)
    .save("foo.bar")
```

<h2 id="merging-on-save">Merging on save</h2>

When saving dataframes or datasets to SingleStore, you can manage how SaveMode.Overwrite is interpreted by the connector via the option overwriteBehavior.
This option can take one of the following values:

1. `dropAndCreate`(default) - drop and create the table before writing new values.
2. `truncate` - truncate the table before writing new values.
3. `merge` - replace rows with new rows by matching on the primary key.
(Use this option only if you need to fully rewrite existing rows with new ones.
If you need to specify some rule for update, use `onDuplicateKeySQL` option instead.)

All these options are case-insensitive.

### Example of `merge` option

Suppose you have the following table, and the `Id` column is the primary key.

`SELECT * FROM <table>;`

| Id    | Name          | Age |
| ----- |:-------------:| ---:|
| 1     | Alice         | 20  |
| 2     | Bob           | 25  |
| 3     | Charlie       | 30  |

If you save the following dataframe with `overwriteBehavior = merge`:

| Id    | Name          | Age |
| ----- |:-------------:| ---:|
| 2     | Daniel        | 22  |
| 3     | Eve           | 27  |
| 4     | Franklin      | 35  |

```scala
df.write
    .format("singlestore")
    .option("overwriteBehavior", "merge")
    .mode(SaveMode.Overwrite)
    .save("<yourdb>.<table>")
```

After the save is complete, the table will look like this:
> note: rows with Id=2 and Id=3 were overwritten with new rows <br />
> note: the row with Id=1 was not touched and still exists in the result

`SELECT * FROM <table>;`

| Id    | Name          | Age |
| ----- |:-------------:| ---:|
| 1     | Alice         | 20  |
| 2     | Daniel        | 22  |
| 3     | Eve           | 27  |
| 4     | Franklin      | 35  |

## SQL Pushdown

The `singlestore-spark-connector` has extensive support for rewriting Spark SQL query
plans into standalone SingleStore queries. This allows most of the computation to be
pushed into the SingleStore distributed system without any manual intervention. The
SQL rewrites are enabled automatically, but can be disabled either globally or
per-query using the `disablePushdown` option.

> :warning: SQL Pushdown is either enabled or disabled on the *entire* Spark
> Session.  If you want to run multiple queries in parallel with different
> values of `disablePushdown`, make sure to run them on separate Spark Sessions.

We currently support most of the primary Logical Plan nodes in Spark SQL
including:

 * Project
 * Filter
 * Aggregate
 * Window
 * Join
 * Limit
 * Sort

We also support most Spark SQL expressions.  A full list of supported
operators/functions can be found in the file
[ExpressionGen.scala](src/main/scala/com/singlestore/spark/ExpressionGen.scala).

The best place to look for examples of fully supported queries is in the tests.
Check out this file as a starting point:
[SQLPushdownTest.scala](src/test/scala/com/singlestore/spark/SQLPushdownTest.scala).

### Debugging SQL Pushdown

If you encounter an issue with SQL Pushdown the first step is to look at the
explain.  You can do this easily from any dataframe using the function
`df.explain()`.  If you pass the argument `true` you will get a lot more output
that includes pre and post optimization passes.

In addition, the `singlestore-spark-connector` outputs a lot of helpful information
when the TRACE log level is enabled for the `com.singlestore.spark` package.  You can
do this in your log4j configuration by adding the following line:

```
log4j.logger.com.singlestore.spark=TRACE
```

Make sure not to leave it in place since it generates a huge amount of tracing
output.

## Parallel Read Support

If you enable parallel reads via the `enableParallelRead` option, the
`singlestore-spark-connector` will attempt to read results directly from SingleStore leaf
nodes.  This can drastically improve performance in some cases.

**:warning: Parallel reads are not consistent**

Parallel reads read directly from partitions on the leaf nodes which skips our
entire transaction layer. This means that the individual reads will see an
independent version of the databases distributed state. Make sure to take this
into account when enabling parallel read.

**:warning: Parallel reads transparently fallback to single stream reads**

Parallel reads currently only work for query-shapes which do no work on the
Aggregator and thus can be pushed entirely down to the leaf nodes. To determine
if a particular query is being pushed down you can ask the dataframe how many
partitions it has like so:

```scala
df.rdd.getNumPartitions
```

If this value is > 1 then we are reading in parallel from leaf nodes.

**:warning: Parallel reads require consistent authentication and connectible leaf nodes**

In order to use parallel reads, the username and password provided to the
`singlestore-spark-connector` must be the same across all nodes in the cluster.

In addition, the hostnames and ports listed by `SHOW LEAVES` must be directly
connectible from Spark.

## Running SQL queries
The methods `executeSinglestoreQuery(query: String, variables: Any*)` and `executeSinglestoreQueryDB(db: String, query: String, variables: Any*)`
allow you to run SQL queries on a SingleStore database directly using the existing `SparkSession` object. The method `executeSinglestoreQuery`
uses the database defined in the `SparkContext` object you use. `executeSinglestoreQueryDB` allows you to specify the database that
will be used for querying.
The following examples demonstrate their usage (assuming you already have
initialized `SparkSession` object named `spark`). The methods return `Iterator[org.apache.spark.sql.Row]` object.

```scala
// this imports the implicit class QueryMethods which adds the methods
// executeSinglestoreQuery and executeSinglestoreQueryDB to SparkSession class
import com.singlestore.spark.SQLHelper.QueryMethods

// You can pass an empty database to executeSinglestoreQueryDB to connect to SingleStore without specifying a database.
// This allows you to create a database which is defined in the SparkSession config for example.
spark.executeSinglestoreQueryDB("", "CREATE DATABASE foo")
// the next query can be used if the database field has been specified in spark object   
s = spark.executeSinglestoreQuery("CREATE TABLE user(id INT, name VARCHAR(30), status BOOLEAN)")

// you can create another database
spark.executeSinglestoreQuery("CREATE DATABASE bar")
// the database specified as the first argument will override the database set in the SparkSession object
s = spark.executeSinglestoreQueryDB("bar", "CREATE TABLE user(id INT, name VARCHAR(30), status BOOLEAN)")
```

You can pass query parameters to the functions as arguments following `query`. The supported types for parameters are `String, Int, Long, Short, Float, Double, Boolean, Byte, java.sql.Date, java.sql.Timestamp`.
```scala
import com.singlestore.spark.SQLHelper.QueryMethods

var userRows = spark.executeSinglestoreQuery("SELECT id, name FROM USER WHERE id > ? AND status = ? AND name LIKE ?", 10, true, "%at%")

for (row <- userRows) {
  println(row.getInt(0), row.getString(1))
}
```
Alternatively, these functions can take `SparkSession` object as the first argument, as in the example below
```scala
import com.singlestore.spark.SQLHelper.{executeSinglestoreQuery, executeSinglestoreQueryDB}

executeSinglestoreQuery(spark, "CREATE DATABASE foo")
var s = executeSinglestoreQueryDB(spark, "foo", "SHOW TABLES")
```
## Security

### Connecting with a Kerberos-authenticated User

You can use the SingleStore Spark Connector with a Kerberized user without any additional configuration.
To use a Kerberized user, you need to configure the connector with the given SingleStore database user that is authenticated with Kerberos
(via the `user` option). Please visit our documentation [here](https://docs.singlestore.com/latest/guides/security/authentication/kerberos-authentication)
to learn about how to configure SingleStore users with Kerberos.

Here is an example of configuring the Spark connector globally with a Kerberized SingleStore user called `krb_user`.

```scala
spark = SparkSession.builder()
    .config("spark.datasource.singlestore.user", "krb_user")
    .getOrCreate()
```

You do not need to provide a password when configuring a Spark Connector user that is Kerberized. 
The connector driver (MariaDB) will be able to authenticate the Kerberos user from the cache by the provided username. 
Other than omitting a password with this configuration, using a Kerberized user with the Connector is no different than using a standard user. 
Note that if you do provide a password, it will be ignored.

### SQL Permissions

SingleStore has a [permission matrix](https://docs.singlestore.com/latest/reference/sql-reference/security-management-commands/permissions-matrix/)
which describes the permissions required to run each command.

To make any SQL operations through Spark connector you should have different
permissions for different type of operation. The matrix below describes the
minimum permissions you should have to perform some operation. As alternative to
minimum required permissions, `ALL PRIVILEGES` allow you to perform any operation.

| Operation                       | Min. Permission          | Alternative Permission |
| ------------------------------- |:------------------------:| ----------------------:|
| `READ` from collection          | `SELECT`                 | `ALL PRIVILEGES`       |
| `WRITE` to collection           | `SELECT, INSERT`         | `ALL PRIVILEGES`       |
| `DROP` database or collection   | `SELECT, INSERT, DROP`   | `ALL PRIVILEGES`       |
| `CREATE` database or collection | `SELECT, INSERT, CREATE` | `ALL PRIVILEGES`       |

For more information on GRANTING privileges, see this [documentation](https://docs.singlestore.com/latest/reference/sql-reference/security-management-commands/grant/)

### SSL Support

The SingleStore Spark Connector uses the MariaDB JDBC Driver under the hood and thus
supports SSL configuration out of the box. In order to configure SSL, first
ensure that your SingleStore cluster has SSL configured. Documentation on how to set
this up can be found here:
https://docs.singlestore.com/latest/guides/security/encryption/ssl/

Once you have setup SSL on your server, you can enable SSL via setting the following options:

```scala
spark.conf.set("spark.datasource.singlestore.useSSL", "true")
spark.conf.set("spark.datasource.singlestore.serverSslCert", "PATH/TO/CERT")
```

**Note:** the `serverSslCert` option may be server's certificate in DER form, or the server's
CA certificate. Can be used in one of 3 forms:

* `serverSslCert=/path/to/cert.pem` (full path to certificate)
* `serverSslCert=classpath:relative/cert.pem` (relative to current classpath)
* or as verbatim DER-encoded certificate string `------BEGIN CERTIFICATE-----...`

You may also want to set these additional options depending on your SSL configuration:

```scala
spark.conf.set("spark.datasource.singlestore.trustServerCertificate", "true")
spark.conf.set("spark.datasource.singlestore.disableSslHostnameVerification", "true")
```

More information on the above parameters can be found at MariaDB's documentation
for their JDBC driver here:
https://mariadb.com/kb/en/about-mariadb-connector-j/#tls-parameters

## Filing issues

When filing issues please include as much information as possible as well as any
reproduction steps. It's hard for us to reproduce issues if the problem depends
on specific data in your SingleStore table for example.  Whenever possible please try
to construct a minimal reproduction of the problem and include the table
definition and table contents in the issue.

If the issue is related to SQL Pushdown (or you aren't sure) make sure to
include the TRACE output (from the com.singlestore.spark package) or the full explain
of the plan.  See the debugging SQL Pushdown section above for more information
on how to do this.

Happy querying!

## Setting up development environment

 * install Oracle JDK 8 from this url: https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html
 * install the community edition of Intellij IDEA from https://www.jetbrains.com/idea/
 * clone the repository https://github.com/memsql/singlestore-spark-connector.git
 * in Intellij IDEA choose `Configure->Plugins` and install Scala plugin
 * in Intellij IDEA run `Import Project` and select path to singlestore-spark-connector `build.sbt` file
 * choose `import project from external model` and `sbt`
 * in `Project JDK` select `New...->JDK` and choose the path to the installed JDK
 * `Finish`
 * it will overwrite some files and create build files (which are in .gitignore)
 * you may need to remove the `.idea` directory for IDEA to load the project properly
 * in Intellij IDEA choose `File->Close Project`
 * run `git checkout .` to revert all changes made by Intellij IDEA
 * in Intellij IDEA choose `Open` and select path to singlestore-spark-connector
 * run `Test Spark 3.0` (it should succeed)

## SQL Pushdown Incompatibilities
 * `ToUnixTimestamp` and `UnixTimestamp` handle only time less then `2038-01-19 03:14:08`, if they get `DateType` or `TimestampType` as a first argument
 * `FromUnixTime` with default format (`yyyy-MM-dd HH:mm:ss`) handle only time less then `2147483648` (`2^31`)
 * `DecimalType` on the overflow is truncated (by default spark either throws exception or returns null)
 * `greatest` and `least` return null if at least one argument is null (in spark these functions skip nulls)
 *  When value can not be converted to numeric or fractional type MemSQL returns 0 (spark returns `null`)
 * `Atanh(x)`, for x ∈ (-∞, -1] ∪ [1, ∞) retuns, `null` (spark returns `NaN`)
 *  When string is casted to numeric type, singlestore takes the prefix of it which is numeric (spark returns `null` if the whole string is not numeric)
 *  When numeric type is casted to the smaller one singlestore truncates it. For example `500` casted to the Byte will be `127`
 Note: spark optimizer can optimize casts for literals and then behaviour for them will match custom spark behaviour
 * When fractional type is casted to integral type singlestore rounds it to the closest value
 * `Log` instead of `NaN`, `Infinity`, `-Infinity` returns `null`
 * `Round` rounds down, if the number that should be rounded is followed by 5 and it is `DOUBLE` or `FLOAT` (`DECIMAL` will be rounded up)
 * `Conv` works differently if the number contains non alphanumeric characters
 * `ShiftLeft`, `ShiftRight` and `ShiftRightUnsigned` converts the value to the UNSIGNED BIGINT and then produces the shift
 In the case of overflow, it returns 0 (`1<<64` = `0` and `10>>20` = `0`)

## Major changes from the 2.0.0 connector

The SingleStore Spark Connector 3.1.1 has a number of key features and enhancements:

* Introduces SQL Optimization & Rewrite for most query shapes and compatible expressions
* Implemented as a native Spark SQL plugin
* Supports both the DataSource and DataSourceV2 API for maximum support of current and future functionality
* Contains deep integrations with the Catalyst query optimizer
* Is compatible with Spark 2.3, 2.4 and 3.0
* Leverages SingleStore LOAD DATA to accelerate ingest from Spark via compression, vectorized cpu instructions, and optimized segment sizes
* Takes advantage of all the latest and greatest features in SingleStore 7.x

<h2 id="java-python-example">Java & Python Examples</h2>

### Java

#### Configuration

```
SparkConf conf = new SparkConf();
conf.set("spark.datasource.singlestore.ddlEndpoint", "singlestore-master.cluster.internal")
conf.set("spark.datasource.singlestore.dmlEndpoints", "singlestore-master.cluster.internal,singlestore-child-1.cluster.internal:3307")
conf.set("spark.datasource.singlestore.user", "admin")
conf.set("spark.datasource.singlestore.password", "s3cur3-pa$$word")
```

#### Read Data

```
DataFrame df = spark
  .read()
  .format("singlestore")
  .option("ddlEndpoint", "singlestore-master.cluster.internal")
  .option("user", "admin")
  .load("foo");
```

#### Write Data

```
df.write()
    .format("singlestore")
    .option("loadDataCompression", "LZ4")
    .option("overwriteBehavior", "dropAndCreate")
    .mode(SaveMode.Overwrite)
    .save("foo.bar")
```

### Python

#### Configuration

```
spark.conf.set("spark.datasource.singlestore.ddlEndpoint", "singlestore-master.cluster.internal")
spark.conf.set("spark.datasource.singlestore.dmlEndpoints", "singlestore-master.cluster.internal,singlestore-child-1.cluster.internal:3307")
spark.conf.set("spark.datasource.singlestore.user", "admin")
spark.conf.set("spark.datasource.singlestore.password", "s3cur3-pa$$word")
```

#### Read Data

```
df = spark \
  .read \
  .format("singlestore") \
  .option("ddlEndpoint", "singlestore-master.cluster.internal") \
  .option("user", "admin") \
  .load("foo")
```

#### Write Data

```
df.write \
    .format("singlestore") \
    .option("loadDataCompression", "LZ4") \
    .option("overwriteBehavior", "dropAndCreate") \
    .mode("overwrite") \
    .save("foo.bar")
```
