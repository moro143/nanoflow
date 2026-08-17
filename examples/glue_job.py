"""A Glue (Spark) job script using nanoflow. Runs locally too if pyspark is installed:

    python examples/glue_job.py --JOB_NAME dev --input ./data --output ./out

Ship nanoflow-core to Glue via `--additional-python-modules nanoflow-core`.
"""
from nanoflow import Flow, task
from nanoflow.aws import glue_args, glue_run_id

args = glue_args("JOB_NAME", "input", "output")

try:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(args.get("JOB_NAME", "dev")).getOrCreate()
except ImportError:  # allow the file to import without spark for docs/tests
    spark = None


@task(retries=2)
def read(path: str):
    return spark.read.json(path)


@task
def clean(df):
    return df.filter("amount > 0")


@task
def write(df, out: str) -> str:
    df.write.mode("overwrite").parquet(out)
    return out


if __name__ == "__main__":
    with Flow("orders-transform", run_id=glue_run_id(), params=args) as f:
        df = read(args["input"])
        write(clean(df), args["output"])
    f.run()
