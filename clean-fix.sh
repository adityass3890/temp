#!/bin/bash
cd /home/adm_akethar1/proj1-main

echo "=================== CLEAN FIX FOR SHELL SCRIPT CONTAMINATION ==================="

echo "=== Step 1: Remove contaminated directories ==="
rm -rf src/

echo "=== Step 2: Create clean SBT structure ==="
mkdir -p src/main/scala/com/tmobile/{common,dlmExtract,dlmIngestion}

echo "=== Step 3: Copy only legitimate Scala files ==="
# Function to check if file is actually Scala code
copy_if_scala() {
    local srcfile="$1"
    local destdir="$2"
    
    if [ -f "$srcfile" ]; then
        # Check if it's not a shell script and contains Scala keywords
        if ! head -1 "$srcfile" | grep -q "#!/bin/bash" && \
           head -10 "$srcfile" | grep -qE "(package|import|object|class)"; then
            echo "✅ Copying legitimate Scala file: $srcfile"
            cp "$srcfile" "$destdir/"
        else
            echo "❌ Skipping shell script: $srcfile"
        fi
    fi
}

# Copy files selectively
for file in cassandraToOracle/src/main/scala/com/tmobile/common/*.scala; do
    copy_if_scala "$file" "src/main/scala/com/tmobile/common"
done

for file in cassandraToOracle/src/main/scala/com/tmobile/dlmExtract/*.scala; do
    copy_if_scala "$file" "src/main/scala/com/tmobile/dlmExtract"
done

for file in cassandraToOracle/src/main/scala/com/tmobile/dlmIngestion/*.scala; do
    copy_if_scala "$file" "src/main/scala/com/tmobile/dlmIngestion"
done

echo "=== Step 4: Verify critical files ==="
ls -la src/main/scala/com/tmobile/dlmExtract/stocktransferorderExtract.scala || echo "ERROR: stocktransferorderExtract.scala missing"
ls -la src/main/scala/com/tmobile/dlmIngestion/stocktransferorderIngest.scala || echo "ERROR: stocktransferorderIngest.scala missing"

echo "=== Step 5: Verify file content ==="
if [ -f "src/main/scala/com/tmobile/dlmExtract/stocktransferorderExtract.scala" ]; then
    echo "First 3 lines of stocktransferorderExtract.scala:"
    head -3 src/main/scala/com/tmobile/dlmExtract/stocktransferorderExtract.scala
fi

echo "=== Step 6: Clean compilation ==="
sbt clean compile package

echo "=== Step 7: Update JAR ==="
cp target/scala-2.12/*.jar jar/tetra-elevate-conversion_2.12-1.0.jar

echo "=== Step 8: Verify JAR contents ==="
jar -tf jar/tetra-elevate-conversion_2.12-1.0.jar | grep stocktransferorder || echo "Classes still missing"
jar -tf jar/tetra-elevate-conversion_2.12-1.0.jar | grep -c "\.class$"

echo "=== Step 9: Test migration ==="
./scripts/dlm_stocktransferorderExtract.sh SUPPLY_CHAIN

