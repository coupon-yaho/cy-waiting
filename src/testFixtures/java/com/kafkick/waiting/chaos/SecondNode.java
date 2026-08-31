package com.kafkick.waiting.chaos;

import java.time.Duration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * 같은 레디스를 보는 <b>두 번째 게이트웨이</b>를 같은 JVM 에 띄운다 (CY-858).
 *
 * <p>하트비트 항목만 심는 것으로는 분모밖에 못 흔든다. 리더 승계·유령 리더의
 * 배분·시계 스큐는 두 번째 제어 평면이 실제로 돌아야 만들어진다.
 */
public final class SecondNode implements AutoCloseable {

    private final ConfigurableApplicationContext context;

    private SecondNode(ConfigurableApplicationContext context) {
        this.context = context;
    }

    /**
     * 띄운다. 포트는 0 이라 겹치지 않고, 노드 식별자는 제어 평면이 스스로
     * 만들므로 첫 노드와 자연히 갈린다.
     *
     * @param 주인 진입점 클래스. 픽스처가 프로덕션 패키지를 안 가져오게 받는다
     */
    public static SecondNode 띄운다(Class<?> 주인, String redisUrl, String backendUri,
            boolean 스케줄러) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(주인)
                .web(WebApplicationType.REACTIVE)
                .profiles("test")
                // **명령줄 인자로 준다.** properties() 는 기본 프로퍼티라
                // 우선순위가 가장 낮아, test 프로파일의 yml 이 그대로 이긴다 —
                // 스케줄러를 켰다고 믿는 채로 제어 평면이 없는 노드가 뜬다.
                .run(
                        "--server.port=0",
                        "--management.server.port=0",
                        "--spring.data.redis.url=" + redisUrl,
                        "--waiting.backend.uri=" + backendUri,
                        "--waiting.scheduler.enabled=" + 스케줄러);
        return new SecondNode(context);
    }

    /**
     * 이 노드가 리더인가. 두 노드가 동시에 참이면 그 구간이 스플릿 브레인이다.
     *
     * <p><b>죽이는 것은 여기서 안 만든다.</b> 컨텍스트를 닫으면 종료 훅이 락을
     * 곱게 놓아 승계가 즉시 일어난다 — 그건 장애 경로가 아니다. 리스가 남는
     * 판은 {@link LeaderFaults} 가 만든다.
     */
    public boolean 리더인가() {
        return 리더_여부();
    }

    /** 이 노드가 쥔 판 번호. 리더가 아니면 0 이다. */
    public long 판_번호() {
        return (long) 메서드를_부른다("fence");
    }

    /**
     * <b>곱게 내린다.</b> 락이 즉시 풀려 다음 리더가 바로 선다 — 죽는 것과
     * 다르므로 장애 경로를 재려면 {@link #죽인다()} 를 쓴다.
     */
    public void 내린다() {
        context.close();
    }

    /** 기동이 끝나 제어 평면이 실제로 도는지. 안 기다리면 분모가 아직 1 이다. */
    public boolean 준비됐나() {
        return context.isRunning();
    }

    @Override
    public void close() {
        if (context.isActive()) {
            context.close();
        }
    }

    /** 컨텍스트에서 빈을 꺼낸다. 픽스처가 프로덕션 타입을 안 가져오게 이름으로 짚는다. */
    public <T> T 빈(String 이름, Class<T> 타입) {
        return context.containsBean(이름) ? context.getBean(이름, 타입) : null;
    }

    private boolean 리더_여부() {
        return (boolean) 메서드를_부른다("isLeader");
    }

    /** 이 노드가 쓴 하트비트의 주인 이름. 분모에 실제로 들어갔는지 볼 때 쓴다. */
    public String ownerId() {
        return (String) 메서드를_부른다("ownerId");
    }

    private Object 메서드를_부른다(String 이름) {
        Object leadership = context.getBean("leadership");
        try {
            return leadership.getClass().getMethod(이름).invoke(leadership);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("리더십에서 %s 를 못 불렀다".formatted(이름), e);
        }
    }

    /**
     * 이 노드의 서비스 포트. 요청을 이쪽으로 보낼 때 쓴다.
     *
     * <p>{@code local.server.port} 는 시험 컨텍스트에만 있는 값이라 여기서는
     * 안 잡힌다. 웹 서버에 직접 묻는다.
     */
    public int port() {
        try {
            Object server = context.getClass().getMethod("getWebServer").invoke(context);
            return (int) server.getClass().getMethod("getPort").invoke(server);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("두 번째 노드의 포트를 못 읽었다", e);
        }
    }

    /** 기동 예산. 두 번째 컨텍스트는 첫 것보다 빠르지만 여유를 둔다. */
    public static Duration 기동_예산() {
        return Duration.ofSeconds(60);
    }
}
