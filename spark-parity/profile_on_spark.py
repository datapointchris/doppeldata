"""Profile DATA with engine spark in the session spark-submit started, and save it to OUT, for check-profiler-parity.sh.

profile_on_spark.py DATA OUT POLICY
"""

import sys
import time

import doppeldata as sd
from pyspark.sql import SparkSession


def main() -> None:
    data, out, policy = sys.argv[1:4]
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    started = time.monotonic()
    sd.profile(data, engine='spark', policy=policy).save(out)
    print(f'Spark {spark.version} profiled {data} in {time.monotonic() - started:.1f} s', file=sys.stderr)


if __name__ == '__main__':
    main()
