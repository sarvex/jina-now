#!/bin/bash

# update script
cp elasticsearch.yml /usr/share/elasticsearch/config/elasticsearch.yml

# Run the docker entrypoint script as the elasticsearch user
su - elasticsearch -c "/usr/local/bin/docker-entrypoint.sh"