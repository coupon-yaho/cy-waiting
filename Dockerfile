# 부하 하네스가 게이트웨이를 띄우는 데 쓴다. 배포 이미지는 아직 아니다.
#
# **JAR 를 밖에서 만들어 넣는다.** 여기서 gradle 을 돌리면 부하 잡이 빌드를
# 한 번 더 하게 되고, 그 둘이 다른 산출물을 쓸 여지가 생긴다.
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app
COPY build/libs/*.jar app.jar

# 컨테이너 메모리에 맞춘다. 안 걸면 JVM 이 호스트 램의 1/4 을 잡으려 든다.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70"

EXPOSE 8080 8081
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
