package com.kafkick.waiting;

/**
 * 픽스처 소스셋의 존재 증명.
 *
 * <p>여기 있는 것은 <b>프로덕션 클래스패스에 들어가지 않는다.</b>
 * 도달 불가 상태를 만드는 생성자가 운영 코드에 노출되지 않게 하려는 것이고
 * (TS-3), 동시에 여러 테스트 소스셋에서 재사용할 수 있다.
 */
public final class Fixtures {

    /** 소스셋 배선 확인용. 실제 픽스처는 Phase 2 에서 들어온다. */
    public static String 소스셋이_연결되었다() {
        return "testFixtures";
    }

    private Fixtures() {
    }
}
