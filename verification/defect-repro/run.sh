#!/usr/bin/env bash
# Reproduces the defects reported in reports/defect-analysis-2026-09-10.md against the current build.
#
#   verification/defect-repro/run.sh
#
# Exit code 0 means every reported defect reproduced (the library is defective, not the report).
# Exit code 1 means at least one reported defect did NOT reproduce — the report needs revisiting.
set -euo pipefail

cd "$(dirname "$0")/../.."

mvn -q compile
mvn -q dependency:build-classpath -Dmdep.outputFile=target/defect-repro-cp.txt -DincludeScope=test

classpath="target/classes:$(cat target/defect-repro-cp.txt)"
mkdir -p target/defect-repro
javac -encoding UTF-8 -cp "$classpath" -d target/defect-repro verification/defect-repro/DefectRepro.java
java -cp "target/defect-repro:$classpath" DefectRepro
