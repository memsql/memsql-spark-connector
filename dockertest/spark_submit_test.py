import time
import sys
import subprocess

def do_test_spark_submit(local_context, test_name, num_aggs=0, num_leaves=1, args=[]):
    ctx = local_context()
    ctx.run_ops()
    ctx.deploy_memsql_cluster(num_aggs=num_aggs, num_leaves=num_leaves, port=10000)
    print "MemSQL up!"
    ctx.deploy_spark()
    print "sleep(30)"
    # wait for spark to be deployed
    time.sleep(30)
    # and then kill the spark interface so that we have spark resources to run a job
    ctx.stop_ops()
    ctx.kill_spark_interface()

    print "Running the job"
    resp = ctx.spark_submit(test_name, extra_args=args)
    print("STDOUT: \n%s" % resp.output)
    print("STDERR: \n%s" % resp.stderr_output)

    assert resp.return_code == 0, "the test failed with return code %d" % resp.return_code

def test_save_to_memsql_very_basic(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestSaveToMemSQLVeryBasic")

def test_memsql_dataframe_very_basic(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestMemSQLDataFrameVeryBasic")

def test_create_with_keys(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestCreateWithKeys")

def test_memsql_types(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestMemSQLTypes")

def test_memsql_types_keyless(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestMemSQLTypes", args=["keyless"])

def test_memsql_context(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestMemSQLContextVeryBasic", num_aggs=2, num_leaves=2)

def test_leaked_conns(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestLeakedConns")

def test_save_errors(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestSaveToMemSQLErrors")

def test_save_rdd_errors(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestSaveToMemSQLWithRDDErrors")

def test_save_json_column(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestSaveToMemSQLJSONColumn")