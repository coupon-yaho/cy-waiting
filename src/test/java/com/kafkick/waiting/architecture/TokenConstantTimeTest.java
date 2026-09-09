package com.kafkick.waiting.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 키를 여럿 볼 때 <b>걸린 시간이 어느 키였는지를 안 알려 준다</b> (CY-902).
 *
 * <p>기능 시험으로는 못 잡는다. 단락 평가로 바꿔도 답은 같고 갈리는 것은 시간뿐이라,
 * 회전 시험 전부가 초록인 채로 부채널이 열린다. 그래서 글자로 못 박는다.
 */
class TokenConstantTimeTest {

    private static final Path SIGNER =
            Path.of("src/main/java/com/kafkick/waiting/domain/queue/SignedToken.java");

    /** 옛 키를 도는 자리. 이 안에서 일찍 빠져나가면 키 수가 밖에서 보인다. */
    private static final Pattern LOOP =
            Pattern.compile("for \\(byte\\[\\] old : alsoAccept\\) \\{(.*?)\\n {8}\\}",
                    Pattern.DOTALL);

    @Test
    @DisplayName("옛_키_반복이_일찍_안_빠져나간다")
    void 옛_키_반복이_일찍_안_빠져나간다() throws IOException {
        Matcher loop = LOOP.matcher(Files.readString(SIGNER));

        assertThat(loop.find()).as("옛 키를 도는 자리를 못 찾았다 — 이름이 바뀌었다").isTrue();
        String body = loop.group(1);
        assertThat(body).as("맞은 뒤에도 나머지를 다 본다")
                .contains("|=")
                .doesNotContain("break")
                .doesNotContain("return")
                .doesNotContain("continue")
                .doesNotContain("||");
    }
}
