# beehave

To run:
./pants binary beehave-app/src/main/scala/com/beehave/api:bin
java -jar dist/server.jar

Example requests:
curl http://localhost:8888/test
curl http://localhost:8888/student?id=1
curl -XPOST --data '{"name": "John Smith", "grade": "6"}' http://localhost:8888/student
curl -XPOST --data '{"label": "test event", id: 1}' http://localhost:8888/event
