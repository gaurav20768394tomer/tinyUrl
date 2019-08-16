docker login

For node-tool
docker exec -it <> nodetool -u cassandra -pw cassandra status

For schema creation:
tinyUrl-docker gtomer$ docker cp src/main/conf/schema.cql edfdac47b810:/opt/
tinyUrl-docker gtomer$ docker exec -it cinst1 cqlsh -f /opt/schema.cql


for build and image
mvn clean package && docker build -t tinyurl .

for up/down
docker-compose -f src/main/conf/docker-compose.yml up/down

For scale:
docker-compose -f src/main/conf/docker-compose.yml up --scale cassandra2=2

URL to access
http://localhost:8089/swagger-ui.html

