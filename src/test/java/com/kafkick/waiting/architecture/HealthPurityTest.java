package com.kafkick.waiting.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 헬스 판정이 <b>의존성을 안 본다.</b>
 *
 * <p>레디스나 뒷단 상태를 넣으면 공유 장애가 전 노드 동시 이탈로 번진다. 설정의
 * 그룹만으로 지키면 배선이 바뀔 때 조용히 깨지므로 <b>타입으로 못박는다.</b>
 */
class HealthPurityTest {

    private static JavaClasses classes;

    @BeforeAll
    static void 읽는다() {
        classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("com.kafkick.waiting");
    }

    @Test
    @DisplayName("헬스는_레디스를_모른다")
    void 헬스는_레디스를_모른다() {
        // 레디스가 흔들릴 때 전 노드가 한꺼번에 빠지면 그게 100% 장애다.
        noClasses()
                .that().haveSimpleNameEndingWith("Health")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.springframework.data.redis..",
                        "io.lettuce..",
                        "com.kafkick.waiting.adapter..")
                .check(classes);
    }

    @Test
    @DisplayName("헬스는_뒷단_서킷을_모른다")
    void 헬스는_뒷단_서킷을_모른다() {
        // 서킷이 열려도 큐와 종결 응답은 해야 한다. 여기 넣으면 뒷단 장애가
        // 게이트웨이 이탈로 번진다.
        noClasses()
                .that().haveSimpleNameEndingWith("Health")
                .should().dependOnClassesThat().haveSimpleNameContaining("CircuitBreaker")
                .check(classes);
    }
}
