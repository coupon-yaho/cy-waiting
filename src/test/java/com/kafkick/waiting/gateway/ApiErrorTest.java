package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
            assertThat(ApiError.Code.SOLD_OUT.status()).isEqualTo(HttpStatus.CONFLICT);
            assertThat(ApiError.Code.SOLD_OUT.code()).isEqualTo("COUPON-306");
            assertThat(ApiError.Code.SOLD_OUT.message()).isEqualTo("쿠폰 재고가 모두 소진되었습니다.");
        }

        /**
         * 뒷단은 발급 경로에서 {@code COUPON-301} 을 낸다. 명세서의
         * {@code COMMON-002} 를 쓰면 재료를 못 믿는 구간에 흘려보낸 요청만
         * 뒷단 코드를 받아, 그 차이가 fail-open 이 열린 순간을 알려 준다.
         */
        @Test
        @DisplayName("없는_쿠폰은_뒷단_발급_경로의_코드를_쓴다")
        void 없는_쿠폰은_뒷단_발급_경로의_코드를_쓴다() {
            assertThat(ApiError.Code.UNKNOWN_COUPON.status()).isEqualTo(HttpStatus.NOT_FOUND);
            assertThat(ApiError.Code.UNKNOWN_COUPON.code()).isEqualTo("COUPON-301");
            assertThat(ApiError.Code.UNKNOWN_COUPON.message())
                    .isEqualTo("쿠폰 회차를 찾을 수 없습니다.");
        }

        @Test
        @DisplayName("잘못된_요청은_뒷단_코드와_문구를_그대로_쓴다")
        void 잘못된_요청은_뒷단_코드와_문구를_그대로_쓴다() {
            assertThat(ApiError.Code.INVALID_REQUEST.status()).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(ApiError.Code.INVALID_REQUEST.code()).isEqualTo("COMMON-001");
            assertThat(ApiError.Code.INVALID_REQUEST.message()).isEqualTo("잘못된 요청입니다.");
        }

        /**
         * 뒷단이 못 내는 상황이라 카탈로그에 없다. 그래도 상태와 코드를 못 박는다
         * — 아무도 안 보면 조용히 바뀌고, 그때 클라이언트가 분기를 잃는다.
         */
        @Test
        @DisplayName("게이트웨이만_내는_셋도_고정한다")
        void 게이트웨이만_내는_셋도_고정한다() {
            assertThat(ApiError.Code.QUEUE_FULL.status())
                    .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
            assertThat(ApiError.Code.QUEUE_FULL.code()).isEqualTo("QUEUE_FULL");
            assertThat(ApiError.Code.RETRY_TOKEN.status())
                    .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
            assertThat(ApiError.Code.RETRY_TOKEN.code()).isEqualTo("RETRY_TOKEN");
            assertThat(ApiError.Code.TEMPORARILY_UNAVAILABLE.status())
                    .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
            assertThat(ApiError.Code.TEMPORARILY_UNAVAILABLE.code())
                    .isEqualTo("TEMPORARILY_UNAVAILABLE");
        }

        /** 뒷단도 내는 코드에 우리만 헤더를 더 달면 그 존재로 갈린다. */
        @Test
        @DisplayName("뒷단도_내는_코드를_구별해_둔다")
        void 뒷단도_내는_코드를_구별해_둔다() {
            assertThat(ApiError.Code.SOLD_OUT.mirrorsBackend()).isTrue();
            assertThat(ApiError.Code.UNKNOWN_COUPON.mirrorsBackend()).isTrue();
            assertThat(ApiError.Code.INVALID_REQUEST.mirrorsBackend()).isTrue();
            assertThat(ApiError.Code.INTERNAL.mirrorsBackend()).isTrue();
            assertThat(ApiError.Code.QUEUE_FULL.mirrorsBackend()).isFalse();
            assertThat(ApiError.Code.RETRY_TOKEN.mirrorsBackend()).isFalse();
            assertThat(ApiError.Code.TEMPORARILY_UNAVAILABLE.mirrorsBackend()).isFalse();
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
            error.write(exchange, ApiError.Code.SOLD_OUT).block();

            // 뒷단은 non_null 로 직렬화해 data 키 자체가 없다. 우리만 쓰면
            // 그 한 글자로 게이트웨이가 끊은 것이 드러난다.
            assertThat(본문(exchange)).isEqualTo("""
                    {"success":false,"error":{"status":409,\
                    "code":"COUPON-306","message":"쿠폰 재고가 모두 소진되었습니다.",\
                    "requestId":"%s","timestamp":"2026-08-18T05:00:12.482Z"}}"""
                    .formatted(exchange.getResponse().getHeaders().getFirst(ApiError.REQUEST_ID)));
        }

        @Test
        @DisplayName("본문의_requestId_가_응답_헤더와_같다")
        void 본문의_requestId_가_응답_헤더와_같다() {
            MockServerWebExchange exchange = 요청();
            error.write(exchange, ApiError.Code.SOLD_OUT).block();

            // 형식까지 본다. 앞뒤가 일관되기만 하면 통과하는 시험이라, 대시
            // 붙은 UUID 로 되돌아가도 안 걸린다 — 뒷단은 대시를 뗀다.
            String header = exchange.getResponse().getHeaders().getFirst(ApiError.REQUEST_ID);
            assertThat(header).matches("[0-9a-f]{32}");
            assertThat(본문(exchange)).contains("\"requestId\":\"%s\"".formatted(header));
        }

        /** 뒷단이 그렇게 한다. 안 받으면 클라이언트가 요청을 못 이어 붙인다. */
        @Test
        @DisplayName("받은_requestId_가_안전하면_그대로_쓴다")
        void 받은_requestId_가_안전하면_그대로_쓴다() {
            MockServerWebExchange exchange = MockServerWebExchange.from(
                    MockServerHttpRequest.get("/api/v1/coupons/c1/entry")
                            .header(ApiError.REQUEST_ID, "8f2c1d4ba7f04e0e9b2c66a1f0d3e551"));
            error.write(exchange, ApiError.Code.SOLD_OUT).block();

            assertThat(exchange.getResponse().getHeaders().getFirst(ApiError.REQUEST_ID))
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
                            .header(ApiError.REQUEST_ID, "\"><script>"));
            error.write(exchange, ApiError.Code.SOLD_OUT).block();

            assertThat(exchange.getResponse().getHeaders().getFirst(ApiError.REQUEST_ID))
                    .isNotEqualTo("\"><script>")
                    .matches("[0-9a-f]{32}");
        }

        @Test
        @DisplayName("본문이_실제로_파싱되는_JSON_이다")
        void 본문이_실제로_파싱되는_JSON_이다() {
            MockServerWebExchange exchange = 요청();
            error.write(exchange, ApiError.Code.SOLD_OUT).block();

            assertThat(exchange.getResponse().getHeaders().getContentType())
                    .isEqualTo(MediaType.APPLICATION_JSON);
            // 헤더만 보면 이름값을 못 한다. 손으로 짠 본문이라 실제로 파싱해 본다.
            assertThatCode(() -> JsonMapper.builder().build().readTree(본문(exchange)))
                    .doesNotThrowAnyException();
        }

        /**
         * 형식을 벗어난 값은 본문에도 안 들어가야 한다. 헤더만 보면 손으로 짠
         * JSON 이 깨진 채로 나가도 통과한다.
         */
        @Test
        @DisplayName("거절한_requestId_는_본문에도_안_들어간다")
        void 거절한_requestId_는_본문에도_안_들어간다() {
            MockServerWebExchange exchange = MockServerWebExchange.from(
                    MockServerHttpRequest.get("/api/v1/coupons/c1/entry")
                            .header(ApiError.REQUEST_ID, "\"><script>"));
            error.write(exchange, ApiError.Code.SOLD_OUT).block();

            assertThat(본문(exchange)).doesNotContain("script");
            assertThatCode(() -> JsonMapper.builder().build().readTree(본문(exchange)))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("requestId_길이_경계를_뒷단과_같게_본다")
        void requestId_길이_경계를_뒷단과_같게_본다() {
            assertThat(추적키("a".repeat(64))).isEqualTo("a".repeat(64));
            // 다르기만 하면 잘라 쓰거나 빈 값이어도 통과한다. 새로 만든 형식까지 본다.
            assertThat(추적키("a".repeat(65))).matches("[0-9a-f]{32}");
        }

        private String 추적키(String 받은_값) {
            MockServerWebExchange exchange = MockServerWebExchange.from(
                    MockServerHttpRequest.get("/api/v1/coupons/c1/entry")
                            .header(ApiError.REQUEST_ID, 받은_값));
            error.write(exchange, ApiError.Code.SOLD_OUT).block();
            return exchange.getResponse().getHeaders().getFirst(ApiError.REQUEST_ID);
        }
    }

    @Nested
    @DisplayName("헤더")
    class Headers {

        /**
         * 프록시가 캐시하면 뒤에 온 사람이 남의 답을 받는다. 순번은 사람마다 다르다.
         */
        @Test
        @DisplayName("게이트웨이만_내는_응답은_캐시를_금지한다")
        void 게이트웨이만_내는_응답은_캐시를_금지한다() {
            MockServerWebExchange exchange = 요청();
            error.write(exchange, ApiError.Code.QUEUE_FULL, 7).block();

            assertThat(exchange.getResponse().getHeaders().getCacheControl())
                    .isEqualTo("no-store");
        }

        /**
         * 뒷단은 캐시 헤더를 안 단다. 우리만 달면 <b>그 헤더 하나로</b> 매진을
         * 게이트웨이가 끊었는지 뒷단까지 갔는지 알 수 있다.
         */
        @Test
        @DisplayName("뒷단도_내는_응답에는_없는_헤더를_안_단다")
        void 뒷단도_내는_응답에는_없는_헤더를_안_단다() {
            MockServerWebExchange exchange = 요청();
            error.write(exchange, ApiError.Code.SOLD_OUT).block();

            assertThat(exchange.getResponse().getHeaders().getCacheControl()).isNull();
        }

        /** 언제 다시 오라는 말이 없으면 각자 마음대로 돌아온다. */
        @Test
        @DisplayName("다시_와도_되는_때를_알려_준다")
        void 다시_와도_되는_때를_알려_준다() {
            MockServerWebExchange exchange = 요청();
            error.write(exchange, ApiError.Code.QUEUE_FULL, 7).block();

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
            error.write(exchange, ApiError.Code.SOLD_OUT).block();

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
            Set<String> 본문들 = ConcurrentHashMap.newKeySet();
            List<Throwable> 터진_것 = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch 출발 = new CountDownLatch(1);
            CountDownLatch 도착 = new CountDownLatch(스레드);
            ExecutorService pool = Executors.newFixedThreadPool(스레드);
            try {
                for (int t = 0; t < 스레드; t++) {
                    // 절반은 깨진 문구로 폴백을 친다. 폴백은 공유 플래그를
                    // 건드리는 유일한 자리라 여기가 실제 경합점이다.
                    boolean 깨뜨린다 = t % 2 == 0;
                    pool.execute(() -> {
                        try {
                            출발.await();
                            for (int i = 0; i < 반복; i++) {
                                MockServerWebExchange exchange = 요청();
                                if (깨뜨린다) {
                                    error.write(exchange, HttpStatus.CONFLICT, "COUPON-306",
                                            "따옴표 \" 가 든 문구", ApiError.NO_RETRY, true).block();
                                } else {
                                    error.write(exchange, ApiError.Code.SOLD_OUT).block();
                                }
                                발급된_것.add(exchange.getResponse().getHeaders()
                                        .getFirst(ApiError.REQUEST_ID));
                                본문들.add(본문(exchange).replaceAll(
                                        "\"requestId\":\"[^\"]*\"", ""));
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Throwable e) {
                            // 삼키면 크기 불일치로만 간접 노출된다.
                            터진_것.add(e);
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

            assertThat(터진_것).isEmpty();
            assertThat(발급된_것).hasSize(스레드 * 반복);
            // 폴백을 여럿이 동시에 쳐도 본문은 두 가지뿐이다.
            assertThat(본문들).hasSize(2);
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

            error.write(exchange, ApiError.Code.SOLD_OUT).block();

            assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.OK);
            // 상태만 보면 본문이 덧붙었는지는 안 보인다.
            assertThat(본문(exchange)).isEmpty();
        }

        /**
         * 본문을 못 만들면 아무것도 안 내는 것이 최악이다 — 클라이언트가 시간
         * 초과까지 매달린다.
         */
        @Test
        @DisplayName("본문을_못_만들면_500_을_낸다")
        void 본문을_못_만들면_500_을_낸다() {
            MockServerWebExchange exchange = 요청();

            error.write(exchange, HttpStatus.CONFLICT, "COUPON-306",
                    "따옴표 \" 가 든 문구", 30, true).block();

            assertThat(exchange.getResponse().getStatusCode())
                    .isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
            // **글자를 적어 못 박는다.** 상수와 비교하면 그 상수가 깨진 JSON 이든
            // 뒷단 봉투와 어긋나든 통과한다 — 다른 모든 것이 실패했을 때 유일하게
            // 나가는 본문이라 아무도 안 보면 그대로 나간다.
            assertThat(본문(exchange)).isEqualTo("""
                    {"success":false,"error":{"status":500,"code":"COMMON-004",\
                    "message":"일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",\
                    "requestId":"%s","timestamp":"2026-08-18T05:00:12.482Z"}}"""
                    .formatted(exchange.getResponse().getHeaders().getFirst(ApiError.REQUEST_ID)));
        }

        /** 못 만든 본문에 재시도 안내를 실으면 500 을 재시도하라고 말하는 셈이다. */
        @Test
        @DisplayName("폴백에는_원래_거절의_헤더를_안_붙인다")
        void 폴백에는_원래_거절의_헤더를_안_붙인다() {
            MockServerWebExchange exchange = 요청();

            error.write(exchange, HttpStatus.TOO_MANY_REQUESTS, "QUEUE_FULL",
                    "따옴표 \" 가 든 문구", 30, false).block();

            assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                    .isNull();
            assertThat(exchange.getResponse().getHeaders().getCacheControl()).isNull();
        }
    }
}
