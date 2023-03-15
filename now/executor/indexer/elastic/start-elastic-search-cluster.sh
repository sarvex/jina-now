#!/bin/bash
# Run the docker entrypoint script as the elasticsearch user
su - elasticsearch -c "/usr/local/bin/docker-entrypoint.sh"