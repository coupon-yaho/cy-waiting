package com.kafkick.waiting.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 도메인 순수성 (DS-1).
 *
 * <p>순수해야 브랜치 100% 와 뮤테이션 테스트가 가능하다. 한 번 깨지면 조용히
 * 번지고, 그때는 되돌리는 비용이 만들 때보다 훨씬 크다.
 */
class DomainPurityTest {

    private static final String DOMAIN = "com.kafkick.waiting.domain..";

    private static JavaClasses classes;

    @BeforeAll
    static void 클래스를_읽는다() {
        classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("com.kafkick.waiting");
    }

    @Test
    @DisplayName("도메인은_Spring을_참조하지_않는다")
    void 도메인은_Spring을_참조하지_않는다() {
        noClasses()
                .that().resideInAPackage(DOMAIN)
                .should().dependOnClassesThat().resideInAnyPackage("org.springframework..")
                .because("도메인이 프레임워크를 알면 순수 단위 테스트가 불가능해진다")
                .check(classes);
    }

    @Test
    @DisplayName("도메인은_Redis를_참조하지_않는다")
    void 도메인은_Redis를_참조하지_않는다() {
        noClasses()
                .that().resideInAPackage(DOMAIN)
                .should().dependOnClassesThat()
                .resideInAnyPackage("io.lettuce..", "redis..", "org.redisson..")
                .because("포트는 도메인이 정의하고 어댑터가 구현한다 (DS-6)")
                .check(classes);
    }

    @Test
    @DisplayName("도메인은_시계를_직접_읽지_않는다")
    void 도메인은_시계를_직접_읽지_않는다() {
        noClasses()
                .that().resideInAPackage(DOMAIN)
                .should().callMethod(System.class, "currentTimeMillis")
                .orShould().callMethod(System.class, "nanoTime")
                .because("시각을 직접 읽으면 초 경계 동작을 시험할 수 없다 (TS-4)")
                .check(classes);
    }

    @Test
    @DisplayName("도메인은_난수를_직접_만들지_않는다")
    void 도메인은_난수를_직접_만들지_않는다() {
        noClasses()
                .that().resideInAPackage(DOMAIN)
                .should().callMethod(Math.class, "random")
                .because("난수원을 주입받아야 재현 가능한 실패를 만들 수 있다 (DS-1)")
                .check(classes);
    }

    @Test
    @DisplayName("도메인은_리액터를_참조하지_않는다")
    void 도메인은_리액터를_참조하지_않는다() {
        noClasses()
                .that().resideInAPackage(DOMAIN)
                .should().dependOnClassesThat().resideInAnyPackage("reactor..")
                .because("판정은 동기 계산이다. 리액티브 타입이 섞이면 시험이 어려워진다")
                .check(classes);
    }
}
