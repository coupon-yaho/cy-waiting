package com.kafkick.waiting.gateway;

import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.NimbusReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import reactor.core.publisher.Flux;

/**
 * 설정에서 검증기를 만든다. <b>알고리즘을 고정한다</b> — 토큰 머리의 알고리즘을 믿으면 HMAC 과 RSA 를
 * 바꿔치기하는 공격이 선다. JWKS 는 비동기로 받아 캐시하므로 요청 경로가 블로킹 호출에 안 묶인다.
 */
public final class JwtDecoders {

    private JwtDecoders() {
    }

    public static ReactiveJwtDecoder of(AuthProperties.Jwt settings, Clock clock) {
        NimbusReactiveJwtDecoder decoder = build(settings);
        decoder.setJwtValidator(validator(settings, clock));
        return decoder;
    }

    static NimbusReactiveJwtDecoder build(AuthProperties.Jwt settings) {
        if (settings.hmac()) {
            MacAlgorithm alg = MacAlgorithm.from(settings.algorithm());
            SecretKeySpec key = new SecretKeySpec(
                    settings.secret().getBytes(StandardCharsets.UTF_8), alg.getName());
            return NimbusReactiveJwtDecoder.withSecretKey(key).macAlgorithm(alg).build();
        }
        SignatureAlgorithm alg = SignatureAlgorithm.from(settings.algorithm());
        if (settings.jwksUri() != null) {
            return NimbusReactiveJwtDecoder.withJwkSetUri(settings.jwksUri()).jwsAlgorithm(alg)
                    .build();
        }
        boolean ec = settings.algorithm().startsWith("ES");
        PublicKey key = publicKey(settings.publicKey(), ec ? "EC" : "RSA");
        if (!ec) {
            return NimbusReactiveJwtDecoder.withPublicKey((RSAPublicKey) key)
                    .signatureAlgorithm(alg).build();
        }
        // EC 공개키는 빌더가 직접 안 받는다. 키 하나짜리 원천으로 넘기되, 선택기가 kid 로 거르므로
        // 토큰의 kid 를 이름표로 붙인다. 키가 하나라 kid 는 고르는 데 안 쓰인다.
        ECPublicKey ecKey = (ECPublicKey) key;
        Curve curve = Curve.forECParameterSpec(ecKey.getParams());
        return NimbusReactiveJwtDecoder.withJwkSource(signed -> Flux.just(
                        new ECKey.Builder(curve, ecKey).keyID(signed.getHeader().getKeyID()).build()))
                .jwsAlgorithm(alg).build();
    }

    static OAuth2TokenValidator<Jwt> validator(AuthProperties.Jwt settings, Clock clock) {
        List<OAuth2TokenValidator<Jwt>> all = new ArrayList<>();
        JwtTimestampValidator timestamps = new JwtTimestampValidator(settings.clockSkew());
        timestamps.setClock(clock);
        all.add(timestamps);
        if (settings.issuer() != null) {
            all.add(new JwtIssuerValidator(settings.issuer()));
        }
        if (settings.audience() != null) {
            all.add(new JwtClaimValidator<List<String>>("aud",
                    aud -> aud != null && aud.contains(settings.audience())));
        }
        return new DelegatingOAuth2TokenValidator<>(all);
    }

    static PublicKey publicKey(String pem, String family) {
        String body = pem.replaceAll("-----(BEGIN|END) PUBLIC KEY-----", "").replaceAll("\\s", "");
        try {
            return KeyFactory.getInstance(family)
                    .generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(body)));
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new IllegalArgumentException("waiting.auth.jwt.public-key 를 못 읽었다", e);
        }
    }
}
