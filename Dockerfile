# 부하 하네스가 게이트웨이를 띄우는 데 쓴다. 배포 이미지는 아직 아니다.
#
# **JAR 를 밖에서 만들어 넣는다.** 여기서 gradle 을 돌리면 부하 잡이 빌드를
# 한 번 더 하게 되고, 그 둘이 다른 산출물을 쓸 여지가 생긴다.
FROM eclipse-temurin:21-jre-alpine

# **root 로 안 돈다.** 컨테이너가 뚫렸을 때 호스트로 번지는 폭이 달라진다.
RUN addgroup -S app && adduser -S -G app app
USER app

WORKDIR /app
# **이름을 정확히 짚는다.** 여럿과 맞으면 무엇이 들어갈지 순서에 달리고,
# 픽스처가 든 것이 실릴 수도 있다. 무시 목록에 기대면 새 산출물이 생길 때 샌다.
ARG JAR=build/libs/waiting-0.4.0.jar
COPY --chown=app:app ${JAR} app.jar

# 컨테이너 메모리에 맞춘다. 안 걸면 JVM 이 호스트 램의 1/4 을 잡으려 든다.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70"

# 관리 포트는 안 적는다. 적어 두면 `-P` 로 띄울 때 밖에서 열리고, 그 포트에는
# 인증이 없어 진단 정보와 종료 조작이 닿는다.
EXPOSE 8080

# **이미지 자신이 건강을 말한다.** compose 에만 두면 다른 데서 띄울 때 사라지고,
# 뜨자마자 트래픽을 받아 재료 없는 상태로 판정한다.
HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=3 \
    CMD wget -qO- http://localhost:8081/actuator/health/liveness || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
