package com.kafkick.waiting.routing;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import org.springframework.cloud.client.ServiceInstance;

/**
 * 고르는 순간 자리를 잡아 둔 인스턴스.
 *
 * <p><b>상한을 읽고 나중에 세면 늦다.</b> 동시 요청이 다 같이 빈자리를 보고
 * 같은 대를 고른 뒤에야 세어지므로, 상한 1 인 대에 둘이 들어간다.
 */
// 표를 여기 실어 필터가 놓는다. 고르는 쪽과 놓는 쪽이 갈려 있어 다른 길이 없다.
public final class ReservedInstance implements ServiceInstance {

    private final ServiceInstance delegate;

    private final InFlightRegistry.Ticket ticket;

    private ReservedInstance(ServiceInstance delegate, InFlightRegistry.Ticket ticket) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.ticket = Objects.requireNonNull(ticket, "ticket");
    }

    public static ReservedInstance of(ServiceInstance delegate, InFlightRegistry.Ticket ticket) {
        return new ReservedInstance(delegate, ticket);
    }

    /** 잡아 둔 자리를 놓는다. <b>어느 경로로 끝나든</b> 불러야 한다. */
    public void release() {
        ticket.finished();
    }

    @Override
    public String getInstanceId() {
        return delegate.getInstanceId();
    }

    @Override
    public String getServiceId() {
        return delegate.getServiceId();
    }

    @Override
    public String getHost() {
        return delegate.getHost();
    }

    @Override
    public int getPort() {
        return delegate.getPort();
    }

    @Override
    public boolean isSecure() {
        return delegate.isSecure();
    }

    @Override
    public URI getUri() {
        return delegate.getUri();
    }

    @Override
    public Map<String, String> getMetadata() {
        return delegate.getMetadata();
    }
}
