package com.kafkick.waiting.domain.coupon;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 운영자가 붙여 넣는 문자열에서 값 하나를 꺼낸다.
 *
 * <p>이 파서가 막는 실패 모드는 조용하다 — 잘못 읽어도 예외가 안 나고 대체값으로
 * 넘어가므로, 운영자는 값을 바꿨다고 믿는데 아무것도 안 바뀐다. 반대로 중첩 객체
 * 안의 같은 이름을 집으면 <b>안 바꾼 값이 바뀐다.</b>
 */
@Tag("unit")
class TunableValuesTest {

    private final TunableValues 값 = TunableValues.create();

    private double 비율(String json) {
        return 값.ratio(json, "idleCreditRatio", -1);
    }

    private long 초(String json) {
        return 값.seconds(json, "inFlightSeconds", -1);
    }

    @Nested
    @DisplayName("경계")
    class Bounds {

        @Test
        @DisplayName("비율의_양끝은_받는다")
        void 비율의_양끝은_받는다() {
            assertThat(비율("{\"idleCreditRatio\":" + Tunables.MIN_IDLE_RATIO + "}"))
                    .isEqualTo(Tunables.MIN_IDLE_RATIO);
            assertThat(비율("{\"idleCreditRatio\":" + Tunables.MAX_IDLE_RATIO + "}"))
                    .isEqualTo(Tunables.MAX_IDLE_RATIO);
        }

        /** 양끝 바로 밖은 버린다. 보호를 끄는 값이 대체값으로 안 넘어가면 안 된다. */
        @Test
        @DisplayName("비율의_양끝_밖은_버린다")
        void 비율의_양끝_밖은_버린다() {
            assertThat(비율("{\"idleCreditRatio\":0.09}")).isEqualTo(-1);
            assertThat(비율("{\"idleCreditRatio\":0.91}")).isEqualTo(-1);
        }

        @Test
        @DisplayName("초의_양끝은_받는다")
        void 초의_양끝은_받는다() {
            assertThat(초("{\"inFlightSeconds\":" + Tunables.MIN_INFLIGHT_SECONDS + "}"))
                    .isEqualTo(Tunables.MIN_INFLIGHT_SECONDS);
            assertThat(초("{\"inFlightSeconds\":" + Tunables.MAX_INFLIGHT_SECONDS + "}"))
                    .isEqualTo(Tunables.MAX_INFLIGHT_SECONDS);
        }

        @Test
        @DisplayName("초의_양끝_밖은_버린다")
        void 초의_양끝_밖은_버린다() {
            assertThat(초("{\"inFlightSeconds\":1}")).isEqualTo(-1);
            assertThat(초("{\"inFlightSeconds\":16}")).isEqualTo(-1);
        }

        /** 소수는 잘라서 받는다. 2.9 를 버리면 운영자가 왜 안 먹는지 모른다. */
        @Test
        @DisplayName("초는_소수를_잘라_받는다")
        void 초는_소수를_잘라_받는다() {
            assertThat(초("{\"inFlightSeconds\":2.9}")).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("최상위 멤버만 본다")
    class TopLevelOnly {

        /**
         * 중첩 객체 안의 같은 이름을 집으면 <b>안 바꾼 값이 바뀐다.</b>
         */
        @Test
        @DisplayName("중첩된_같은_이름은_안_집는다")
        void 중첩된_같은_이름은_안_집는다() {
            assertThat(비율("{\"other\":{\"idleCreditRatio\":0.5}}")).isEqualTo(-1);
        }

        /** 중첩 블록은 통째로 건너뛰고 그 뒤의 최상위 멤버를 찾는다. */
        @Test
        @DisplayName("중첩_블록_뒤의_멤버를_찾는다")
        void 중첩_블록_뒤의_멤버를_찾는다() {
            assertThat(비율("{\"other\":{\"a\":1},\"idleCreditRatio\":0.5}")).isEqualTo(0.5);
            assertThat(비율("{\"other\":[1,2,{\"x\":3}],\"idleCreditRatio\":0.5}"))
                    .isEqualTo(0.5);
        }

        /** 문자열 안의 괄호를 세면 블록의 끝을 잘못 짚는다. */
        @Test
        @DisplayName("문자열_안의_괄호는_안_센다")
        void 문자열_안의_괄호는_안_센다() {
            assertThat(비율("{\"other\":{\"a\":\"}\"},\"idleCreditRatio\":0.5}"))
                    .isEqualTo(0.5);
        }

        /** 이스케이프된 따옴표를 닫는 따옴표로 세면 그 뒤가 전부 밀린다. */
        @Test
        @DisplayName("이스케이프된_따옴표는_안_센다")
        void 이스케이프된_따옴표는_안_센다() {
            assertThat(비율("{\"other\":\"a\\\"b\",\"idleCreditRatio\":0.5}")).isEqualTo(0.5);
        }

        @Test
        @DisplayName("앞의_멤버를_지나_찾는다")
        void 앞의_멤버를_지나_찾는다() {
            assertThat(비율("{\"a\":1,\"idleCreditRatio\":0.5}")).isEqualTo(0.5);
        }

        @Test
        @DisplayName("없는_키는_대체값이다")
        void 없는_키는_대체값이다() {
            assertThat(비율("{\"a\":1}")).isEqualTo(-1);
        }
    }

    @Nested
    @DisplayName("망가진 입력")
    class BrokenInput {

        /** 객체가 아니면 아무것도 안 읽는다. */
        @Test
        @DisplayName("객체가_아니면_대체값이다")
        void 객체가_아니면_대체값이다() {
            assertThat(비율("[1,2]")).isEqualTo(-1);
            assertThat(비율("")).isEqualTo(-1);
            assertThat(비율("   ")).isEqualTo(-1);
        }

        /** 뒤에 뭐가 붙은 수는 못 믿는다. */
        @Test
        @DisplayName("수_뒤에_글자가_붙으면_버린다")
        void 수_뒤에_글자가_붙으면_버린다() {
            assertThat(비율("{\"idleCreditRatio\":0.5oops}")).isEqualTo(-1);
        }

        /** 수 글자만 있어도 수가 아닐 수 있다. 그 값만 버리고 판은 안 죽인다. */
        @Test
        @DisplayName("수_글자로만_된_쓰레기도_버린다")
        void 수_글자로만_된_쓰레기도_버린다() {
            assertThat(비율("{\"idleCreditRatio\":1e}")).isEqualTo(-1);
            assertThat(비율("{\"idleCreditRatio\":--1}")).isEqualTo(-1);
        }

        @Test
        @DisplayName("문자열_값은_버린다")
        void 문자열_값은_버린다() {
            assertThat(비율("{\"idleCreditRatio\":\"0.5\"}")).isEqualTo(-1);
        }

        /** 닫히지 않은 것들. 여기서 무한 루프에 빠지면 판이 멎는다. */
        @Test
        @DisplayName("안_닫힌_입력은_대체값이다")
        void 안_닫힌_입력은_대체값이다() {
            assertThat(비율("{")).isEqualTo(-1);
            assertThat(비율("{\"idleCreditRatio\"")).isEqualTo(-1);
            assertThat(비율("{\"idleCreditRatio\":")).isEqualTo(-1);
            assertThat(비율("{\"other\":{\"a\":1")).isEqualTo(-1);
            assertThat(비율("{\"other\":\"a")).isEqualTo(-1);
        }

        /**
         * <b>찾는 키의 값 자리</b>가 안 닫힌 블록이어야 한다.
         *
         * <p>다른 이름의 블록이면 이 자리를 안 지난다 — 블록 끝을 -1 이 아니라
         * 0 으로 내는 결함이 그 픽스처에서는 안 드러난다.
         */
        @Test
        @DisplayName("찾는_키의_안_닫힌_블록도_대체값이다")
        void 찾는_키의_안_닫힌_블록도_대체값이다() {
            assertThat(비율("{\"idleCreditRatio\":{\"a\":1")).isEqualTo(-1);
            assertThat(비율("{\"idleCreditRatio\":{\"a\":\"x}")).isEqualTo(-1);
            assertThat(비율("{\"idleCreditRatio\":[1,2")).isEqualTo(-1);
        }

        /**
         * <b>자바가 받아 주는 표기를 우리가 받으면 안 된다.</b>
         *
         * <p>{@code Double.valueOf} 는 리터럴 접미사와 16진 부동소수를 받는다.
         * 글자 화이트리스트가 없으면 운영자가 못 쓴 값이 조용히 통과한다.
         */
        @Test
        @DisplayName("자바_리터럴_표기는_버린다")
        void 자바_리터럴_표기는_버린다() {
            assertThat(비율("{\"idleCreditRatio\":0.5d}")).isEqualTo(-1);
            assertThat(비율("{\"idleCreditRatio\":0.5f}")).isEqualTo(-1);
            assertThat(비율("{\"idleCreditRatio\":0x1.8p-1}")).isEqualTo(-1);
        }

        /** 음의 지수는 받는다. 화이트리스트에서 빼면 이 표기가 조용히 안 먹는다. */
        @Test
        @DisplayName("음의_지수_표기를_받는다")
        void 음의_지수_표기를_받는다() {
            assertThat(비율("{\"idleCreditRatio\":5e-1}")).isEqualTo(0.5);
        }

        /** 콜론이 없으면 이름과 값을 못 가른다. */
        @Test
        @DisplayName("콜론이_없으면_대체값이다")
        void 콜론이_없으면_대체값이다() {
            assertThat(비율("{\"idleCreditRatio\" 0.5}")).isEqualTo(-1);
        }

        /** 쉼표가 없으면 그 뒤는 없는 것으로 본다. */
        @Test
        @DisplayName("쉼표가_없으면_뒤를_안_본다")
        void 쉼표가_없으면_뒤를_안_본다() {
            assertThat(비율("{\"a\":1 \"idleCreditRatio\":0.5}")).isEqualTo(-1);
        }

        /** 공백은 어디에 있어도 지난다. */
        @Test
        @DisplayName("공백을_지난다")
        void 공백을_지난다() {
            assertThat(비율("  {  \"idleCreditRatio\"  :  0.5  }  ")).isEqualTo(0.5);
        }
    }
}
