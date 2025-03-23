from pyspark import SparkConf


def get_spark_conf(app_name):
    return (
        SparkConf()
        .setMaster("local[*]")
        .setAppName(app_name)
        .set("spark.default.parallelism", "4")
        .set(
            "spark.driver.extraJavaOptions",
            "-XX:+UseG1GC -XX:+UseStringDeduplication -XX:+DisableExplicitGC -XX:+UseCompressedOops",
        )
        .set(
            "spark.executor.extraJavaOptions",
            "-XX:+UseG1GC -XX:+UseStringDeduplication -XX:+DisableExplicitGC -XX:+UseCompressedOops",
        )
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "1g")
        .set("spark.memory.fraction", "0.7")
        .set("spark.memory.storageFraction", "0.3")
        .set("spark.driver.bindAddress", "127.0.0.1")
        .set("spark.driver.host", "localhost")
        .set("spark.ui.enabled", "false")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "128m")
        .set("spark.kryoserializer.buffer", "32m")
    )
