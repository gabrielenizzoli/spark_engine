docker run \
  -v $(pwd):/opt/spark/work-dir \
  --rm datamechanics/spark:jvm-only-3.1.1-latest /opt/spark/bin/spark-submit \
  --properties-file ./spark.properties \
  --class sparkengine.plan.app.Start spark-internal \
  @./app.properties
