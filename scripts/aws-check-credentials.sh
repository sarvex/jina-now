#!/bin/sh

for file in $(git diff-index --cached --name-only HEAD); do
    if grep -q -E "(AKIA|ASIA)[A-Z0-9]{8,40}" $file; then
        echo "AWS key ID found in $file. Commit denied."
        exit 1
    fi
done
