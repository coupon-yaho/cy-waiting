package com.kafkick.waiting;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** testFixtures 소스셋이 테스트에서 보이는지 확인한다. */
class FixturesWiringTest {

    @Test
    @DisplayName("픽스처_소스셋을_테스트에서_쓸_수_있다")
    void 픽스처_소스셋을_테스트에서_쓸_수_있다() {
        assertThat(Fixtures.소스셋이_연결되었다()).isEqualTo("testFixtures");
    }
}
