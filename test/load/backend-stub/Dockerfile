# 뒷단 쿠폰 서비스 대역. **지연과 동시 한도를 설정으로 받는다** — 그게 없으면
# 인스턴스별 용량 차등(Phase 9)을 만들 수 없다.
FROM node:22-alpine

# root 로 안 돈다. 부하 하네스라도 같은 기준을 쓴다.
USER node

WORKDIR /app
COPY --chown=node:node server.js package.json ./

ENV PORT=8090 LATENCY_MS=0 MAX_INFLIGHT=0
EXPOSE 8090
ENTRYPOINT ["node", "server.js"]
