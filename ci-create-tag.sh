#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -euo pipefail

echo "Fetching tags to ensure we have the latest..."
git fetch --tags

CURRENT_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0.0")
echo "Current tag: $CURRENT_TAG"

# Normalize version (remove 'v' prefix if present)
VERSION=${CURRENT_TAG#v}
echo "Normalized version: $VERSION"

# Split version into parts (expecting MAJOR.MINOR.PATCH.BUILD)
IFS='.' read -ra VERSION_PARTS <<< "$VERSION"

# Ensure we have 4 parts, pad with zeros if needed
while [ ${#VERSION_PARTS[@]} -lt 4 ]; do
  VERSION_PARTS+=(0)
done

MAJOR=${VERSION_PARTS[0]}
MINOR=${VERSION_PARTS[1]}
PATCH=${VERSION_PARTS[2]}
BUILD=${VERSION_PARTS[3]}

# Increment build number
BUILD=$((BUILD + 1))

NEW_VERSION="$MAJOR.$MINOR.$PATCH.$BUILD"
NEW_TAG="v$NEW_VERSION"

echo "New version: $NEW_VERSION"
echo "New tag: $NEW_TAG"

# Check if tag already exists
if git rev-parse "$NEW_TAG" >/dev/null 2>&1; then
  echo "Tag $NEW_TAG already exists. Skipping tag creation."
  echo "NEW_VERSION=$NEW_VERSION" >> $GITHUB_ENV
  exit 0
fi

# Configure git
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

# Create annotated tag with [skip ci] to prevent infinite loop
git tag -a "$NEW_TAG" -m "Release $NEW_VERSION [skip ci]"

# Push tag
git push origin "$NEW_TAG"

# Export version to GitHub environment
echo "NEW_VERSION=$NEW_VERSION" >> $GITHUB_ENV
echo "NEW_VERSION=$NEW_VERSION" >> $GITHUB_OUTPUT

echo "Successfully created and pushed tag: $NEW_TAG"
