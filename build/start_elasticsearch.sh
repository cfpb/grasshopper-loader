CONTAINER_ID=$(docker run -d -p 9200:9200 -p 9300:9300 elasticsearch:1.7.3)
ES_HOST=$(docker inspect "$CONTAINER_ID" | grep IPAddress | cut -d '"' -f 4)
echo "$ES_HOST"
