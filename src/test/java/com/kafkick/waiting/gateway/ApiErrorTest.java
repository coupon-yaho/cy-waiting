package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

/**
 * <b>게이트웨이가 낸 응답인지 뒷단이 낸 응답인지 알 수 없어야 한다.</b>
 * 구별되면 그 차이로 게이트웨이의 존재와 상태를 읽어낼 수 있다.
 */
class ApiErrorTest {

    private static final Instant NOW = Instant.parse("2026-08-18T05:00:12.482Z");

    private final ApiError error = ApiError.of(Clock.fixed(NOW, ZoneOffset.UTC));

    private MockServerWebExchange 요청() {
        return MockServerWebExchange.from(MockServerHttpRequest.get("/api/v1/coupons/c1/entry"));
    }

    private String 본문(MockServerWebExchange exchange) {
        return exchange.getResponse().getBodyAsString().block();
    }

    @Nested
    @DisplayName("뒷단 카탈로그")
    class Catalog {

        /**
         * 매진은 뒷단이 실제로 내는 상황이다. 코드나 문구가 다르면 그 하나로
         * 게이트웨이가 끊었는지 뒷단까지 갔는지 갈린다.
         */
        @Test
        @DisplayName("매진은_뒷단_코드와_문구를_그대로_쓴다")
        void 매진은_뒷단_코드와_문구를_그대로_쓴다() {
            assertThat(ApiError.SOLD_OUT.status()).isEqualTo(HttpStatus.CONFLICT);
            assertThat(ApiError.SOLD_OUT.code()).isEqualTo("COUPON-306");
            assertThat(ApiError.SOLD_OUT.message()).isEqualTo("쿠폰 재고가 모두 소진되었습니다.");
        }

        @Test
        @DisplayName("없는_쿠폰은_뒷단_코드와_문구를_그대로_쓴다")
        void 없는_쿠폰은_뒷단_코드와_문구를_그대로_쓴다() {
            assertThat(ApiError.NOT_FOUND.status()).isEqualTo(HttpStatus.NOT_FOUND);
            assertThat(ApiError.NOT_FOUND.code()).isEqualTo("COMMON-002");
            assertThat(ApiError.NOT_FOUND.message()).isEqualTo("요청한 리소스를 찾을 수 없습니다.");
        }

        @Test
        @DisplayName("잘못된_요청은_뒷단_코드와_문구를_그대로_쓴다")
        void 잘못된_요청은_뒷단_코드와_문구를_그대로_쓴다() {
            assertThat(ApiError.INVALID_REQUEST.status()).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(ApiError.INVALID_REQUEST.code()).isEqualTo("COMMON-001");
            assertThat(ApiError.INVALID_REQUEST.message()).isEqualTo("잘못된 요청입니다.");
        }
    }

    @Nested
    @DisplayName("봉투")
    class Envelope {

        /** 필드 하나만 빠져도 뒷단 응답과 갈린다. 순서까지 같게 낸다. */
        @Test
        @DisplayName("뒷단과_같은_봉투로_낸다")
        void 뒷단과_같은_봉투로_낸다() {
            MockServerWebExchange exchange = 요청();
            error.write(exchange, ApiError.SOLD_OUT).block();

            assertThat(본문(exchange)).isEqualTo("""
                    {"success":false,"data":null,"error":{"status":409,\
                    "code":"COUPON-306","message":"쿠폰 재고가 모두 소진되었습니다.",\
                    "requestId":"%s","timestamp":"2026-08-18T05:00:12.482Z"}}"""
                    .formatted(exchange.getResponse().getHeaders().getFirst("X-Request-Id")));
        }

        @Test
        @DisplayName("본문의_requestId_가_응답_헤더와_같다")
        void 본문의_requestId_가_응답_헤더와_같다() {
            MockServerWebExchange exchange = 요청();
            error.write(exchange, ApiError.SOLD_OUT).block();

            String header = exchange.getResponse().getHeaders().getFirst("X-Request-Id");
            assertThat(header).isNotBlank();
            assertThat(본문(exchange)).contains("\"requestId\":\"%s\"".formatted(header));
        }

        /** 뒷단이 그렇게 한다. 안 받으면 클라이언트가 요청을 못 이어 붙인다. */
        @Test
        @DisplayName("받은_requestId_가_안전하면_그대로_쓴다")
        void 받은_requestId_가_안전하면_그대로_쓴다() {
            MockServerWebExchange exchange = MockServerWebExchange.from(
                    MockServerHttpRequest.get("/api/v1/coupons/c1/entry")
                            .header("X-Request-Id", "8f2c1d4ba7f04e0e9b2c66a1f0d3e551"));
            error.write(exchange, ApiError.SOLD_OUT).block();

            assertThat(exchange.getResponse().getHeaders().getFirst("X-Request-Id"))
                    .isEqualTo("8f2c1d4ba7f04e0e9b2c66a1f0d3e551");
        }

        /**
         * 그대로 되쓰면 응답에 남의 문자열을 싣게 된다. 뒷단도 같은 형식으로
         * 거른다 — 거르는 방식이 다르면 그 차이가 신호가 된다.
         */
        @Test
        @DisplayName("받은_requestId_가_형식을_벗어나면_새로_만든다")
        void 받은_requestId_가_형식을_벗어나면_새로_만든다() {
            MockServerWebExchange exchange = MockServerWebExchange.from(
                    MockServerHttpRequest.get("/api/v1/coupons/c1/entry")
                            .header("X-Request-Id", "\"><script>"));
            error.write(exchange, ApiError.SOLD_OUT).block();

            assertThat(exchange.getResponse().getHeaders().getFirst("X-Request-Id"))
                    .isNotEqualTo("\"><script>")
                    .matches("[0-9a-f]{32}");
        }

        @Test
        @DisplayName("본문이_JSON_이다")
        void 본문이_JSON_이다() {
            MockServerWebExchange exchange = 요청();
            error.write(exchange, ApiError.SOLD_OUT).block();

            assertThat(exchange.getResponse().getHeaders().getContentType())
                    .isEqualTo(MediaType.APPLICATION_JSON);
        }
    }

    @Nested
    @DisplayName("헤더")
    class Headers {

        /**
         * 프록시가 이 응답을 캐시하면 뒤에 온 사람이 남의 답을 받는다. 매진은
         * 재입고로 뒤집히고, 순번은 사람마다 다르다.
         */
        @Test
        @DisplayName("캐시를_금지한다")
        void 캐시를_금지한다() {
            MockServerWebExchange exchange = 요청();
            error.write(exchange, ApiError.SOLD_OUT).block();

            assertThat(exchange.getResponse().getHeaders().getCacheControl())
                    .isEqualTo("no-store");
        }

        /** 언제 다시 오라는 말이 없으면 각자 마음대로 돌아온다. */
        @Test
        @DisplayName("다시_와도_되는_때를_알려_준다")
        void 다시_와도_되는_때를_알려_준다() {
            MockServerWebExchange exchange = 요청();
            error.write(exchange, ApiError.QUEUE_FULL, 7).block();

            assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                    .isEqualTo("7");
        }

        /**
         * 매진은 다시 와도 소용이 없다. 시각을 실으면 재고 없는 쿠폰에
         * 재시도를 부르게 된다.
         */
        @Test
        @DisplayName("매진에는_다시_올_때를_안_싣는다")
        void 매진에는_다시_올_때를_안_싣는다() {
            MockServerWebExchange exchange = 요청();
            error.write(exchange, ApiError.SOLD_OUT).block();

            assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                    .isNull();
        }
    }

    @Nested
    @DisplayName("동시성")
    class Concurrency {

        private static final int 스레드 = 32;
        private static final int 반복 = 50;

        /**
         * 인스턴스 하나를 모든 요청이 나눠 쓴다. requestId 가 겹치면 로그와
         * 응답을 잇는 키가 남을 가리키게 된다.
         */
        @Test
        @DisplayName("동시에_써도_requestId_가_안_겹친다")
        void 동시에_써도_requestId_가_안_겹친다() throws InterruptedException {
            Set<String> 발급된_것 = ConcurrentHashMap.newKeySet();
            CountDownLatch 출발 = new CountDownLatch(1);
            CountDownLatch 도착 = new CountDownLatch(스레드);
            ExecutorService pool = Executors.newFixedThreadPool(스레드);
            try {
                for (int t = 0; t < 스레드; t++) {
                    pool.execute(() -> {
                        try {
                            출발.await();
                            for (int i = 0; i < 반복; i++) {
                                MockServerWebExchange exchange = 요청();
                                error.write(exchange, ApiError.SOLD_OUT).block();
                                발급된_것.add(exchange.getResponse().getHeaders()
                                        .getFirst("X-Request-Id"));
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            도착.countDown();
                        }
                    });
                }
                출발.countDown();
                assertThat(도착.await(30, TimeUnit.SECONDS)).isTrue();
            } finally {
                pool.shutdownNow();
            }

            assertThat(발급된_것).hasSize(스레드 * 반복);
        }
    }

    @Nested
    @DisplayName("실패 경로")
    class FailurePath {

        /**
         * 헤더가 이미 나갔으면 상태 코드를 못 바꾼다. 그대로 쓰려 들면 예외가
         * 나고, 그 예외가 원래 실패를 덮는다.
         */
        @Test
        @DisplayName("이미_나간_응답은_되돌리지_않는다")
        void 이미_나간_응답은_되돌리지_않는다() {
            MockServerWebExchange exchange = 요청();
            exchange.getResponse().setStatusCode(HttpStatus.OK);
            exchange.getResponse().setComplete().block();

            error.write(exchange, ApiError.SOLD_OUT).block();

            assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.OK);
        }

        /**
         * 본문을 못 만들면 아무것도 안 내는 것이 최악이다 — 클라이언트가 시간
         * 초과까지 매달린다.
         */
        @Test
        @DisplayName("본문을_못_만들면_500_을_낸다")
        void 본문을_못_만들면_500_을_낸다() {
            MockServerWebExchange exchange = 요청();
            ApiError.Code 깨진_코드 = new ApiError.Code(
                    HttpStatus.CONFLICT, "COUPON-306", "따옴표 \" 가 든 문구");

            error.write(exchange, 깨진_코드).block();

            assertThat(exchange.getResponse().getStatusCode())
                    .isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
            assertThat(본문(exchange))
                    .isEqualTo(new String(ApiError.FALLBACK, StandardCharsets.UTF_8));
        }
    }
}
