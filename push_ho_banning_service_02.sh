#!/bin/bash

# This script pushes the ho_banning_service_02 branch to GitHub
# The branch has been created and is ready to push

set -e

echo "================================================================================"
echo "Push ho_banning_service_02 Branch to GitHub"
echo "================================================================================"
echo ""

# Check if the branch exists
if ! git rev-parse --verify ho_banning_service_02 >/dev/null 2>&1; then
    echo "ERROR: Branch ho_banning_service_02 does not exist locally!"
    echo "Please ensure you are in the correct repository."
    exit 1
fi

echo "✓ Branch ho_banning_service_02 found locally"
echo ""

# Show branch info
echo "Branch Information:"
echo "-------------------"
git log --oneline ho_banning_service_02 -3
echo ""

# Confirm it's one commit on master
COMMIT_COUNT=$(git rev-list --count master..ho_banning_service_02)
echo "Commits on top of master: $COMMIT_COUNT"

if [ "$COMMIT_COUNT" -ne 1 ]; then
    echo "WARNING: Expected 1 commit, found $COMMIT_COUNT"
    echo "Continuing anyway..."
fi
echo ""

# Push the branch
echo "Pushing branch to origin..."
git push origin ho_banning_service_02

echo ""
echo "================================================================================"
echo "✓ SUCCESS! Branch ho_banning_service_02 has been pushed to GitHub"
echo "================================================================================"
echo ""
echo "You can view it at:"
echo "https://github.com/hansieodendaal/nomos/tree/ho_banning_service_02"
echo ""
