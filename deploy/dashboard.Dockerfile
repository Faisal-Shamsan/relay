FROM node:22-bookworm-slim

WORKDIR /app

COPY package.json package-lock.json ./
COPY packages/dashboard ./packages/dashboard
COPY packages/protocol/package.json ./packages/protocol/package.json
COPY packages/server/package.json ./packages/server/package.json
COPY packages/sdk/package.json ./packages/sdk/package.json

RUN npm ci \
  && npm run build -w relay-dashboard

ENV NODE_ENV=production
ENV PORT=3000

WORKDIR /app/packages/dashboard

EXPOSE 3000

CMD ["npx", "next", "start", "--hostname", "0.0.0.0", "--port", "3000"]
