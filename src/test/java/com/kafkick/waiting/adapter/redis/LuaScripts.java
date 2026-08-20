package com.kafkick.waiting.adapter.redis;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Lua 스크립트를 <b>클래스패스에서</b> 읽는다.
 *
 * <p>파일 경로로 읽으면 작업 디렉터리가 모듈 루트일 때만 동작한다. 스크립트가
 * 옮겨지면 읽는 쪽을 전부 따로 고쳐야 하므로 한 곳에 둔다.
 */
final class LuaScripts {

    private LuaScripts() {
    }

    /** @param name {@code enqueue.lua} 처럼 {@code redis/} 아래의 이름 */
    static String of(String name) {
        String path = "redis/" + name;
        try (InputStream in = LuaScripts.class.getClassLoader().getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalArgumentException("클래스패스에 없다: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
