#!/bin/bash
cd /home/adm_akethar1/proj1-main

echo "=================== FINAL COMPLETE SOLUTION ==================="

echo "=== Method 1: Download all dependencies ==="
wget -q https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.6.0/java-driver-core-4.6.0.jar -O lib/java-driver-core-4.6.0.jar
wget -q https://repo1.maven.org/maven2/com/datastax/oss/java-driver-query-builder/4.6.0/java-driver-query-builder-4.6.0.jar -O lib/java-driver-query-builder-4.6.0.jar
wget -q https://repo1.maven.org/maven2/com/datastax/oss/java-driver-mapper-runtime/4.6.0/java-driver-mapper-runtime-4.6.0.jar -O lib/java-driver-mapper-runtime-4.6.0.jar

JAR_LIST="lib/config-1.2.0.jar,lib/spark-cassandra-connector_2.12-3.0.0.jar,lib/java-driver-core-4.6.0.jar,lib/java-driver-query-builder-4.6.0.jar,lib/java-driver-mapper-runtime-4.6.0.jar"

# Update script with all JARs
cp scripts/dlm_stocktransferorderExtract.sh scripts/dlm_stocktransferorderExtract.sh.backup3
sed -i "s|--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 --jars [^[:space:]]*|--jars ${JAR_LIST}|g" scripts/dlm_stocktransferorderExtract.sh
sed -i 's|--conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions|--conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions --conf spark.sql.catalog.mycatalog=com.datastax.spark.connector.datasource.CassandraCatalog|g' scripts/dlm_stocktransferorderExtract.sh

echo "=== Testing with complete dependencies ==="
./scripts/dlm_stocktransferorderExtract.sh SUPPLY_CHAIN

echo "If above fails, trying fat JAR approach..."
if [ $? -ne 0 ]; then
    echo "=== Method 2: Fat JAR approach ==="
    echo 'addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")' >> project/plugins.sbt
    
    cat >> build.sbt << 'EOF'
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat  
  case x => MergeStrategy.first
}
EOF

    sbt clean assembly
    cp target/scala-2.12/*.jar jar/stocktransferorder-fat.jar
    
    # Update script for fat JAR
    sed -i 's|--jars [^[:space:]]*|jar/stocktransferorder-fat.jar|g' scripts/dlm_stocktransferorderExtract.sh
    ./scripts/dlm_stocktransferorderExtract.sh SUPPLY_CHAIN
fi

