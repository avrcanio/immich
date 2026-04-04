FROM node:22-alpine

RUN apk add --no-cache docker-cli

WORKDIR /app

COPY server.js /app/server.js

EXPOSE 8091

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD wget -qO- http://127.0.0.1:8091/healthz >/dev/null || exit 1

CMD ["node", "/app/server.js", "serve"]
