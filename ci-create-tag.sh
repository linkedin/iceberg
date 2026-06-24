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
#!/usr/bin/env bash
set -euo pipefail

git fetch --tags --prune

# Get latest tag (or empty)
CURRENT_TAG="$(git describe --tags --abbrev=0 2>/dev/null || true)"

if [[ -z "$CURRENT_TAG" ]]; then
  echo "No tag found. Aborting tag creation to avoid v0.0.0.x tags."
  exit 1
fi

# strip leading v if present
VERSION="${CURRENT_TAG#v}"
echo "Current tag: $CURRENT_TAG"
echo "Current version: $VERSION"

# Extract only leading numeric part (e.g., 1.2.0 or 1.2.0.4)
NUMERIC_PART="$(echo "$VERSION" | sed -E 's/^([0-9]+(\.[0-9]+){1,3}).*$/\1/')"

# Split components
IFS='.' read -r MAJOR MINOR PATCH BUILD <<< "$(echo ${NUMERIC_PART} | awk -F. '{ for(i=1;i<=4;i++) printf $i (i<4?OFS:"") }' OFS='.')"

# If BUILD empty, set to 0
BUILD=${BUILD:-0}

NEW_BUILD=$((BUILD + 1))
NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}.${NEW_BUILD}"
NEW_TAG="v${NEW_VERSION}"

# Prevent duplicate tag
if git rev-parse "refs/tags/${NEW_TAG}" >/dev/null 2>&1; then
  echo "Tag ${NEW_TAG} already exists. Exiting."
  exit 1
fi

# export for later steps (GitHub Actions)
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  echo "NEW_VERSION=${NEW_VERSION}" >> "${GITHUB_OUTPUT}"
else
  # fallback for older runners
  echo "NEW_VERSION=${NEW_VERSION}" >> "${GITHUB_ENV:-/dev/null}"
fi

# Configure git and push
git config user.name "${GIT_USER_NAME:-CI Builder}"
git config user.email "${GIT_USER_EMAIL:-ci@example.com}"

git tag -a "${NEW_TAG}" -m "Release ${NEW_VERSION} [skip ci]"
git push origin "${NEW_TAG}"
echo "Created and pushed ${NEW_TAG}"