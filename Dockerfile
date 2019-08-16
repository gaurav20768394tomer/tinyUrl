FROM java:8

EXPOSE 8080

ADD /target/tinyurl-1.0-SNAPSHOT.jar tinyurl-1.0-SNAPSHOT.jar

ENTRYPOINT ["java", "-jar", "tinyurl-1.0-SNAPSHOT.jar"]