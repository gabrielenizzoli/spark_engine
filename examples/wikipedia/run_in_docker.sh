IMAGE="datamechanics/spark:jvm-only-3.1.1-latest"

docker pull ${IMAGE}

docker run \
  -v $(pwd):/opt/spark/work-dir -u $(id -u) \
  --rm ${IMAGE} /opt/spark/bin/spark-submit \
  --properties-file ./spark.properties \
  --class sparkengine.plan.app.Start spark-internal \
  @./app.properties
