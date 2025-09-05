#!/bin/bash
cd /home/adm_akethar1/proj1-main

echo "=================== COMPREHENSIVE DIAGNOSIS ==================="

echo "=== Step 1: File verification ==="
ls -la cassandraToOracle/src/main/scala/com/tmobile/dlmExtract/stocktransferorderExtract.scala
ls -la cassandraToOracle/src/main/scala/com/tmobile/dlmIngestion/stocktransferorderIngest.scala

echo "=== Step 2: build.sbt content ==="
cat build.sbt

echo "=== Step 3: SBT source directories ==="
sbt "show Compile/sourceDirectories" 2>/dev/null

echo "=== Step 4: Create standard SBT structure ==="
mkdir -p src/main/scala/com/tmobile/{common,dlmExtract,dlmIngestion}
find cassandraToOracle/src/main/scala -name "*.scala" -exec cp {} src/main/scala/com/tmobile/common/ \; 2>/dev/null || true
cp cassandraToOracle/src/main/scala/com/tmobile/dlmExtract/*.scala src/main/scala/com/tmobile/dlmExtract/ 2>/dev/null || true
cp cassandraToOracle/src/main/scala/com/tmobile/dlmIngestion/*.scala src/main/scala/com/tmobile/dlmIngestion/ 2>/dev/null || true

echo "=== Step 5: Compile with standard structure ==="
sbt clean compile package 2>&1 | tee full-build.log

echo "=== Step 6: Check results ==="
find target/ -name "*.class" | wc -l
jar -tf jar/tetra-elevate-conversion_2.12-1.0.jar | grep stocktransferorder || echo "Still no stocktransferorder classes"

echo "=== Step 7: Test execution ==="
./scripts/dlm_stocktransferorderExtract.sh SUPPLY_CHAIN

