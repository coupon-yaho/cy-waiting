package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 키 이름은 여기서만 만든다 (RD-3).
 *
 * <p>키가 두 곳에서 만들어지면 <b>샤딩을 도입할 때 한쪽만 고쳐진다.</b> 그때는
 * 진행 중인 큐가 통째로 유실된다.
 */
class RedisKeysTest {

    @Test
    @DisplayName("전역_키_문자열이_고정되어_있다")
    void 전역_키_문자열이_고정되어_있다() {
        // 이름이 바뀌면 배포 중 두 판이 서로 다른 키를 본다.
        assertThat(RedisKeys.SNAPSHOT).isEqualTo("gw:snapshot");
        assertThat(RedisKeys.INSTANCES).isEqualTo("gw:instances");
        assertThat(RedisKeys.LEADER).isEqualTo("scheduler:leader");
        assertThat(RedisKeys.TUNABLES).isEqualTo("gw:tunables");
        assertThat(RedisKeys.ACTIVE_COUPONS).isEqualTo("coupons:active");
        assertThat(RedisKeys.COUPON_POLICY).isEqualTo("coupon:policy");
    }

    @Test
    @DisplayName("샤드가_하나면_접미사가_붙지_않는다")
    void 샤드가_하나면_접미사가_붙지_않는다() {
        // 붙였다 떼면 샤딩 도입 순간 콜드 쿠폰 전체의 키가 갈린다.
        assertThat(RedisKeys.queue("c1", 1, 0)).isEqualTo("queue:{c1}");
        assertThat(RedisKeys.maxScore("c1", 1, 0)).isEqualTo("maxscore:{c1}");
        assertThat(RedisKeys.admitted("c1", 1, 0)).isEqualTo("admitted:{c1}");
        assertThat(RedisKeys.grace("c1", 1, 0)).isEqualTo("grace:{c1}");
        assertThat(RedisKeys.alive("c1", 1, 0)).isEqualTo("alive:{c1}");
    }

    @Test
    @DisplayName("샤드가_여럿이면_접미사가_붙는다")
    void 샤드가_여럿이면_접미사가_붙는다() {
        assertThat(RedisKeys.queue("c1", 4, 3)).isEqualTo("queue:{c1:3}");
        assertThat(RedisKeys.maxScore("c1", 4, 3)).isEqualTo("maxscore:{c1:3}");
        assertThat(RedisKeys.alive("c1", 4, 3)).isEqualTo("alive:{c1:3}");
    }

    @Test
    @DisplayName("같은_쿠폰의_키가_모두_같은_해시태그를_쓴다")
    void 같은_쿠폰의_키가_모두_같은_해시태그를_쓴다() {
        // 같은 샤드의 키가 같은 슬롯에 모여야 하나의 Lua 가 원자적으로 다룬다 (RD-2).
        List<String> keys = List.of(
                RedisKeys.queue("c1", 4, 3),
                RedisKeys.maxScore("c1", 4, 3),
                RedisKeys.admitted("c1", 4, 3),
                RedisKeys.grace("c1", 4, 3),
                RedisKeys.alive("c1", 4, 3));

        assertThat(keys).allMatch(k -> RedisKeys.hashTagOf(k).equals("c1:3"));
    }

    @Test
    @DisplayName("재고_키는_샤드와_무관하다")
    void 재고_키는_샤드와_무관하다() {
        // 발급 계층이 소유한다. 샤딩 시 슬롯이 갈리므로 Lua 에서 만지지 않는다.
        assertThat(RedisKeys.stock("c1")).isEqualTo("stock:{c1}");
    }

    @Test
    @DisplayName("쿠폰_식별자에_해시태그_문자가_있으면_거부한다")
    void 쿠폰_식별자에_해시태그_문자가_있으면_거부한다() {
        // 클라이언트 입력이 키 이름에 들어가는 경로는 전부 의심한다 (PK-R5).
        // 중괄호가 섞이면 슬롯이 엉뚱한 곳으로 가고 Lua 가 거부된다.
        assertThatThrownBy(() -> RedisKeys.queue("c{1}", 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RedisKeys.queue("c:1", 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RedisKeys.alive("c{1}", 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RedisKeys.stock("c:1"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("빈_식별자는_거부한다")
    void 빈_식별자는_거부한다() {
        assertThatThrownBy(() -> RedisKeys.queue("", 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RedisKeys.queue(null, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("샤드_번호가_범위를_벗어나면_거부한다")
    void 샤드_번호가_범위를_벗어나면_거부한다() {
        // 범위를 넘으면 아무도 안 보는 키가 생기고 그 큐는 영영 안 빠진다.
        assertThatThrownBy(() -> RedisKeys.queue("c1", 4, 4))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RedisKeys.queue("c1", 4, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("스냅샷_예약_접두사는_쿠폰_ID에_못_들어간다")
    void 스냅샷_예약_접두사는_쿠폰_ID에_못_들어간다() {
        // '#' 는 스냅샷 해시에서 전역값을 가르는 접두사다. 쿠폰 ID 에 들어가면
        // 그 쿠폰이 전역값을 덮어쓴다 — '#credit' 이름의 쿠폰 하나로 전 쿠폰의
        // 몫이 0 이 되고, 한산한 쿠폰이 전부 큐로 간다.
        assertThatThrownBy(() -> RedisKeys.queue("#credit", 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RedisKeys.queue("c#1", 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RedisKeys.stock("#nodes"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
