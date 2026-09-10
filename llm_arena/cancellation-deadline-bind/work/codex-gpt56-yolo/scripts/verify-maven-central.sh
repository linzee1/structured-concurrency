#!/usr/bin/env bash
set -euo pipefail

version="${1:-0.1.0}"
artifact_dir="$HOME/.m2/repository/io/github/huatalk/parallel-in-scope/$version"

# Remove the local copy so this check proves Maven Central can serve the artifact.
rm -rf "$artifact_dir"

for attempt in $(seq 1 12); do
  if mvn -B -ntp -U \
      -f verification/maven-central-consumer/pom.xml \
      -Dparallel-in-scope.version="$version" \
      -Dtest=MavenCentralConsumerTest#publishedArtifactCanBeResolvedAndUsed \
      clean verify; then
    exit 0
  fi

  if [ "$attempt" -lt 12 ]; then
    echo "Maven Central has not served $version yet; retrying in 10 seconds (attempt $((attempt + 1))/12)." >&2
    rm -rf "$artifact_dir"
    sleep 10
  fi
done

exit 1
