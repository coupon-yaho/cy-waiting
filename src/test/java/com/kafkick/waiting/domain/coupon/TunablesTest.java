package com.kafkick.waiting.domain.coupon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * 장애 중에 배포 없이 되돌릴 수 있어야 롤백 전략이 성립합니다.
 *
 * <p><b>잘못된 값이 게이트웨이를 멈추면 안 됩니다.</b> 운영자가 장애 중에 손으로
 * 넣는 값이라 오타가 납니다. 그때 기동이 막히면 되돌릴 수단 자체가 사라집니다.
 */
class TunablesTest {

    /** 값을 안 적었을 때 도는 값. 키가 없어도 게이트웨이는 돌아야 합니다. */
    @Test
    @DisplayName("빈_값이면_기본값이_된다")
    void 빈_값이면_기본값이_된다() {
        Tunables t = Tunables.parse(null);

        assertThat(t).isEqualTo(Tunables.defaults());
    }

    /** 적은 값이 그대로 실려야 합니다. 안 실리면 되돌릴 수단이 없는 것과 같습니다. */
    @Test
    @DisplayName("적은_값이_그대로_실린다")
    void 적은_값이_그대로_실린다() {
        Tunables t = Tunables.parse("{\"idleCreditRatio\":0.5,\"inFlightSeconds\":5}");

        assertThat(t.idleCreditRatio()).isEqualTo(0.5);
        assertThat(t.inFlightSeconds()).isEqualTo(5);
    }

    /**
     * <b>깨진 JSON 이 기동을 막으면 안 됩니다.</b> 장애 중에 손으로 넣는 값이라
     * 오타가 나는데, 그때 멈추면 되돌릴 수단 자체가 사라집니다.
     */
    @Test
    @DisplayName("깨진_값이면_기본값으로_떨어진다")
    void 깨진_값이면_기본값으로_떨어진다() {
        assertThat(Tunables.parse("{깨졌다")).isEqualTo(Tunables.defaults());
        assertThat(Tunables.parse("")).isEqualTo(Tunables.defaults());
    }

    /**
     * <b>범위를 벗어난 값도 마찬가지입니다.</b> 한산 비율이 1 을 넘으면 한산 통과가
     * 노드 예산을 넘어서고, 0 이면 그 경로가 통째로 막힙니다.
     */
    @Test
    @DisplayName("범위를_벗어나면_그_값만_기본값이_된다")
    void 범위를_벗어나면_그_값만_기본값이_된다() {
        Tunables t = Tunables.parse("{\"idleCreditRatio\":2.0,\"inFlightSeconds\":5}");

        // 한 값이 틀렸다고 나머지까지 버리지 않는다. 그러면 오타 하나가
        // 운영자가 방금 고친 다른 값도 되돌린다.
        assertThat(t.idleCreditRatio()).isEqualTo(Tunables.defaults().idleCreditRatio());
        assertThat(t.inFlightSeconds()).isEqualTo(5);
    }

    /** 모르는 키는 무시합니다. 새 값을 먼저 넣어 두고 배포하는 순서가 가능해야 합니다. */
    @Test
    @DisplayName("모르는_키는_무시한다")
    void 모르는_키는_무시한다() {
        Tunables t = Tunables.parse("{\"아직없는값\":1,\"inFlightSeconds\":7}");

        assertThat(t.inFlightSeconds()).isEqualTo(7);
    }

    /**
     * <b>통째로 바뀌어야 합니다.</b> 필드별로 갈아 끼우면 낡은 타임아웃과 새 격벽
     * 상한 같은 조합이 한순간 존재하고, 그 조합은 아무도 검증한 적이 없습니다.
     */
    @Test
    @DisplayName("값들이_한_벌로_움직인다")
    void 값들이_한_벌로_움직인다() {
        Tunables 하나 = Tunables.parse("{\"idleCreditRatio\":0.5,\"inFlightSeconds\":5}");
        Tunables 둘 = Tunables.parse("{\"idleCreditRatio\":0.5,\"inFlightSeconds\":5}");

        assertThat(하나).isEqualTo(둘);
    }

    /** 콜론이 없으면 값이 아니다. 키만 적고 만 경우다. */
    @Test
    @DisplayName("콜론이_없으면_기본값이_된다")
    void 콜론이_없으면_기본값이_된다() {
        assertThat(Tunables.parse("{\"inFlightSeconds\"}"))
                .isEqualTo(Tunables.defaults());
    }

    /** 값 자리가 수가 아니면 그 값만 버린다. */
    @Test
    @DisplayName("수가_아니면_그_값만_버린다")
    void 수가_아니면_그_값만_버린다() {
        Tunables t = Tunables.parse("{\"idleCreditRatio\":\"높게\",\"inFlightSeconds\":4}");

        assertThat(t.idleCreditRatio()).isEqualTo(Tunables.defaults().idleCreditRatio());
        assertThat(t.inFlightSeconds()).isEqualTo(4);
    }

    /** 공백이 끼어도 읽는다. 사람이 손으로 적는 값이다. */
    @Test
    @DisplayName("공백이_있어도_읽는다")
    void 공백이_있어도_읽는다() {
        assertThat(Tunables.parse("{\"inFlightSeconds\" :   9 }").inFlightSeconds())
                .isEqualTo(9);
    }

    /**
     * <b>보호를 끄는 값은 안 받습니다.</b> 0 이면 부하가 없는 쿠폰의 요청이 전부
     * 큐 등록으로 가고, 그게 요청 경로에서 레디스를 치는 유일한 예외 경로입니다 —
     * 값 하나로 피크 전량이 그리로 들어가고, 되돌리려면 그 레디스에 써야 합니다.
     *
     * <p>1 에 가까워도 안 됩니다. 한산과 토큰이 같은 노드 예산을 쓰므로, 한산이
     * 거의 다 긁으면 차례가 온 사람이 밀립니다 (불변식 3).
     */
    @Test
    @DisplayName("보호를_끄는_비율은_안_받는다")
    void 보호를_끄는_비율은_안_받는다() {
        double 기본 = Tunables.defaults().idleCreditRatio();

        for (String 값 : List.of("0", "0.05", "-0.5", "0.95", "1", "2")) {
            assertThat(Tunables.parse("{\"idleCreditRatio\":" + 값 + "}").idleCreditRatio())
                    .as("비율 %s", 값).isEqualTo(기본);
        }
        // 문턱 안은 그대로 받는다. 안 그러면 운영자가 아무것도 못 바꾼다.
        assertThat(Tunables.parse("{\"idleCreditRatio\":0.4}").idleCreditRatio())
                .isEqualTo(0.4);
    }

    /**
     * <b>직접 만드는 길에도 같은 문턱이 있어야 합니다.</b> 읽기만 거르면 NaN 이나
     * 1 이상의 비율이 그대로 들어오고, 그 값은 상한 계산을 통째로 뒤집습니다.
     */
    @Test
    @DisplayName("직접_만들_때도_문턱을_지킨다")
    void 직접_만들_때도_문턱을_지킨다() {
        assertThatThrownBy(() -> new Tunables(Double.NaN, 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Tunables(1.0, 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Tunables(-0.1, 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Tunables(0.0, 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Tunables(0.7, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Tunables(0.7, 61))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>무한대는 좁히면 멀쩡한 값이 됩니다.</b> {@code 1e309} 가 그대로 통과하면
     * {@code Long.MAX_VALUE} 초로 저장되고, 격벽 상한이 사실상 무한이 됩니다.
     */
    @Test
    @DisplayName("long_밖의_초는_안_받는다")
    void long_밖의_초는_안_받는다() {
        long 기본 = Tunables.defaults().inFlightSeconds();

        assertThat(Tunables.parse("{\"inFlightSeconds\":1e309}").inFlightSeconds())
                .isEqualTo(기본);
        assertThat(Tunables.parse("{\"inFlightSeconds\":1e30}").inFlightSeconds())
                .isEqualTo(기본);
    }

    /**
     * <b>서킷보다 짧으면 안 됩니다.</b> 느려진 뒷단의 요청이 서킷에 집계되기 전에
     * 격벽이 먼저 끊으면 서킷이 영영 안 열리고, 그러면 회복 경로 자체가 사라집니다.
     */
    @Test
    @DisplayName("서킷보다_짧거나_격벽을_끄는_초는_안_받는다")
    void 서킷보다_짧거나_격벽을_끄는_초는_안_받는다() {
        long 기본 = Tunables.defaults().inFlightSeconds();

        for (String 값 : List.of("0", "1", "61", "100000")) {
            assertThat(Tunables.parse("{\"inFlightSeconds\":" + 값 + "}").inFlightSeconds())
                    .as("초 %s", 값).isEqualTo(기본);
        }
        assertThat(Tunables.parse("{\"inFlightSeconds\":2}").inFlightSeconds()).isEqualTo(2);
    }

    /**
     * <b>중첩과 배열은 키 자리가 아닙니다.</b> 앞뒤 문자만 보면 블록 안의 같은
     * 이름이 최상위의 진짜 값을 이기고, 최상위가 배열이어도 읽힙니다.
     */
    @Test
    @DisplayName("최상위_객체의_멤버만_키로_본다")
    void 최상위_객체의_멤버만_키로_본다() {
        long 기본 = Tunables.defaults().inFlightSeconds();

        assertThat(Tunables.parse("{\"note\":{\"inFlightSeconds\":99},\"inFlightSeconds\":5}")
                .inFlightSeconds()).as("중첩이 진짜 키를 이기면 안 된다").isEqualTo(5);
        assertThat(Tunables.parse("{\"tags\":[\"a\",\"inFlightSeconds\"],\"other\":42}")
                .inFlightSeconds()).as("배열 원소는 키가 아니다").isEqualTo(기본);
        assertThat(Tunables.parse("[{\"inFlightSeconds\":9}]")
                .inFlightSeconds()).as("최상위가 객체가 아니면 안 읽는다").isEqualTo(기본);
    }

    /** 콜론 뒤가 끊긴 값. 잘려서 저장된 경우다. */
    @Test
    @DisplayName("콜론_뒤가_끊겨도_기본값이_된다")
    void 콜론_뒤가_끊겨도_기본값이_된다() {
        assertThat(Tunables.parse("{\"inFlightSeconds\":"))
                .isEqualTo(Tunables.defaults());
        assertThat(Tunables.parse("{\"inFlightSeconds\":  "))
                .isEqualTo(Tunables.defaults());
    }

    /** 닫는 괄호 없이 수로 끝나도 읽는다. */
    @Test
    @DisplayName("수로_끝나도_읽는다")
    void 수로_끝나도_읽는다() {
        assertThat(Tunables.parse("{\"inFlightSeconds\":8").inFlightSeconds()).isEqualTo(8);
    }

    /**
     * <b>값 안에 든 문자열을 키로 읽으면 안 됩니다.</b> 그러면 아무 문자열이나
     * 적어 넣어 그 뒤의 수를 설정으로 들일 수 있습니다.
     */
    @Test
    @DisplayName("값_안의_문자열은_키가_아니다")
    void 값_안의_문자열은_키가_아니다() {
        // **뒤에 오는 키가 다른 이름이어야 가른다.** 같은 이름을 두면 취약한
        // 파서도 진짜 키의 콜론에 착지해서 답만 맞힌다 — 이유는 틀린 채로.
        Tunables t = Tunables.parse("{\"note\":\"inFlightSeconds\",\"other\":99}");

        assertThat(t.inFlightSeconds()).isEqualTo(Tunables.defaults().inFlightSeconds());
    }

    /**
     * <b>수 뒤에 뭐가 붙었으면 그 값은 못 믿습니다.</b> 앞부분만 읽으면 {@code 8oops}
     * 가 8 로 들어가고, 운영자는 자기가 적은 값이 통과한 줄 압니다.
     */
    @Test
    @DisplayName("수_뒤에_뭐가_붙으면_버린다")
    void 수_뒤에_뭐가_붙으면_버린다() {
        assertThat(Tunables.parse("{\"inFlightSeconds\":8oops}").inFlightSeconds())
                .isEqualTo(Tunables.defaults().inFlightSeconds());
    }

    /**
     * <b>사람이 손으로 적는 값입니다.</b> 들여쓰기와 줄바꿈이 들어간다고 해서
     * 키를 못 찾으면, 운영자는 자기가 적은 값이 왜 안 먹는지 모릅니다.
     */
    @Test
    @DisplayName("보기_좋게_적은_설정도_읽는다")
    void 보기_좋게_적은_설정도_읽는다() {
        Tunables t = Tunables.parse("{\n  \"inFlightSeconds\" : 7 ,\n  \"x\": 1\n}");

        assertThat(t.inFlightSeconds()).isEqualTo(7);
    }

    /**
     * <b>객체가 아니면 설정이 아닙니다.</b> 중괄호 없이 키만 적힌 것을 읽어 주면
     * 형식이 깨진 값이 조용히 통과하고, 그때부터 무엇이 실렸는지 못 믿습니다.
     */
    @Test
    @DisplayName("객체가_아니면_안_읽는다")
    void 객체가_아니면_안_읽는다() {
        assertThat(Tunables.parse("\"inFlightSeconds\":5").inFlightSeconds())
                .isEqualTo(Tunables.defaults().inFlightSeconds());
    }

    /**
     * <b>잘린 값은 전부 기본값으로 떨어집니다.</b> 장애 중에 손으로 넣다 만 값,
     * 복사가 끊긴 값이 실제로 들어옵니다 — 그때 절반만 읽어 들이면 아무도 무엇이
     * 실렸는지 모릅니다.
     */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
        "   ",
        "{",
        "{ \"",
        "{\"inFlightSeconds\"",
        "{\"inFlightSeconds\"}",
        "{\"inFlightSeconds\":",
        "{\"inFlightSeconds\":}",
        "{\"note\":\"x",
        "{\"note\":{",
        "{\"note\":{\"a",
        "{\"note\":1 \"inFlightSeconds\":2}",
        "{\"note\":1,",
    })
    @DisplayName("잘린_값은_기본값으로_떨어진다")
    void 잘린_값은_기본값으로_떨어진다(String 잘린_것) {
        assertThat(Tunables.parse(잘린_것).inFlightSeconds())
                .isEqualTo(Tunables.defaults().inFlightSeconds());
    }

    /**
     * <b>이스케이프된 따옴표는 문자열을 안 끝냅니다.</b> 안 세면 키 이름이 거기서
     * 잘리고, 그 뒤가 통째로 값 자리로 밀려 나머지 설정이 사라집니다.
     */
    @Test
    @DisplayName("이스케이프된_따옴표를_넘어_읽는다")
    void 이스케이프된_따옴표를_넘어_읽는다() {
        assertThat(Tunables.parse("{\"no\\\"te\":\"x\",\"inFlightSeconds\":4}")
                .inFlightSeconds()).isEqualTo(4);
        assertThat(Tunables.parse("{\"note\":\"x\\\"y\",\"inFlightSeconds\":4}")
                .inFlightSeconds()).isEqualTo(4);
    }

    /** 값이 문자열·객체·배열이어도 그 다음 멤버를 이어서 읽는다. */
    @Test
    @DisplayName("어떤_값이_앞에_와도_다음_멤버를_읽는다")
    void 어떤_값이_앞에_와도_다음_멤버를_읽는다() {
        assertThat(Tunables.parse("{\"a\":[1,[2],{\"b\":3}],\"inFlightSeconds\":7}")
                .inFlightSeconds()).isEqualTo(7);
    }
}
