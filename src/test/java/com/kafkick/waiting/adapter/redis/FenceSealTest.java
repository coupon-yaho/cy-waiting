package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FenceSealTest {

    private static final Duration 수명 = Duration.ofHours(1);

    @Test
    @DisplayName("수명에서_남은_수명을_뺀_것이_나이다")
    void 수명에서_남은_수명을_뺀_것이_나이다() {
        assertThat(FenceSeal.of(1, 수명.minusMillis(300).toMillis(), 수명).lastApplyAge())
                .hasValue(Duration.ofMillis(300));
    }

    /** 앞 리더가 더 긴 수명으로 썼으면 음수가 된다. 0 으로 잘라 한 틱을 다 기다린다. */
    @Test
    @DisplayName("남은_수명이_수명보다_길면_나이는_0이다")
    void 남은_수명이_수명보다_길면_나이는_0이다() {
        assertThat(FenceSeal.of(1, 수명.plusMillis(5).toMillis(), 수명).lastApplyAge()).hasValue(Duration.ZERO);
    }

    @Test
    @DisplayName("표가_없거나_수명이_없으면_나이를_모른다")
    void 표가_없거나_수명이_없으면_나이를_모른다() {
        assertThat(FenceSeal.of(1, -2, 수명).lastApplyAge()).isEmpty();
        assertThat(FenceSeal.of(1, -1, 수명).lastApplyAge()).isEmpty();
    }

    @Test
    @DisplayName("합치면_잠근_수는_더하고_나이는_가장_어린_쪽이다")
    void 합치면_잠근_수는_더하고_나이는_가장_어린_쪽이다() {
        FenceSeal 늙은 = new FenceSeal(1, Optional.of(Duration.ofMillis(800)));
        FenceSeal 어린 = new FenceSeal(1, Optional.of(Duration.ofMillis(300)));
        FenceSeal 모름 = new FenceSeal(0, Optional.empty());

        assertThat(늙은.plus(어린)).isEqualTo(new FenceSeal(2, Optional.of(Duration.ofMillis(300))));
        assertThat(어린.plus(늙은).lastApplyAge()).hasValue(Duration.ofMillis(300));
        assertThat(모름.plus(늙은).lastApplyAge()).hasValue(Duration.ofMillis(800));
        assertThat(늙은.plus(모름).lastApplyAge()).hasValue(Duration.ofMillis(800));
        assertThat(FenceSeal.NONE.plus(모름)).isEqualTo(FenceSeal.NONE);
    }
}
