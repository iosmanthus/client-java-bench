FROM openjdk:8
COPY ./target/client-java-bench-1.0-SNAPSHOT-jar-with-dependencies.jar /app.jar
ENTRYPOINT ["java","-cp", "/app.jar", "Bench"]