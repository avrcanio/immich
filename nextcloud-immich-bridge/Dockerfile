FROM node:22-alpine

WORKDIR /app

RUN apk add --no-cache docker-cli

COPY bridge.js /app/bridge.js

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD test -f /state/last-run.json || exit 1

CMD ["node", "/app/bridge.js"]
