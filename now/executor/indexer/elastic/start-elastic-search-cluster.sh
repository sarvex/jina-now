#!/bin/bash

# Add the necessary lines to elasticsearch.yml
echo "xpack.security.enabled: false" >> /usr/share/elasticsearch/config/elasticsearch.yml
echo "xpack.security.transport.ssl.enabled: false" >> /usr/share/elasticsearch/config/elasticsearch.yml
echo "discovery.type: single-node" >> /usr/share/elasticsearch/config/elasticsearch.yml

# Run the docker entrypoint script as the elasticsearch user
su - elasticsearch -c "/usr/local/bin/docker-entrypoint.sh"