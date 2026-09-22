package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 커널 한도가 설정보다 작으면 조용히 작아지는 것을 기동 때 말하는가 (CY-986). */
class KernelBacklogTest {

    @TempDir
    Path 폴더;

    @Test
    @DisplayName("커널 한도가 작으면 실제 크기는 커널 쪽이다")
    void 커널이_작으면() throws Exception {
        Path 한도 = Files.writeString(폴더.resolve("somaxconn"), "4096\n");

        assertThat(AcceptBacklog.effective(16_384, 한도))
                .as("여기만 올리면 아무 일도 안 일어난다 — 그 사실을 알아야 한다").hasValue(4_096);
    }

    @Test
    @DisplayName("커널 한도가 크면 설정값이 그대로다")
    void 커널이_크면() throws Exception {
        Path 한도 = Files.writeString(폴더.resolve("somaxconn"), "65535");

        assertThat(AcceptBacklog.effective(16_384, 한도)).hasValue(16_384);
    }

    @Test
    @DisplayName("진짜 proc 파일을 끝까지 읽는다 — 크기를 0 으로 보고하는 파일이다")
    void 진짜_proc_파일() {
        Path 진짜 = Path.of("/proc/sys/net/core/somaxconn");
        assumeTrue(Files.isReadable(진짜), "리눅스가 아니면 잴 것이 없다");

        // 크기에 기대 읽으면 첫 글자만 온다 — 4096 이 4 가 되어 "커널 한도가 1" 같은 거짓 경고가 난다.
        assertThat(AcceptBacklog.effective(Integer.MAX_VALUE, 진짜).orElseThrow())
                .as("커널이 받는 가장 작은 한도보다 작으면 잘려 읽은 것이다").isGreaterThanOrEqualTo(128);
    }

    @Test
    @DisplayName("못 읽으면 모른다고 한다 — 리눅스가 아닐 수 있다")
    void 못_읽으면() {
        assertThat(AcceptBacklog.effective(16_384, 폴더.resolve("없다"))).isEmpty();
    }
}
