FROM node:22-bookworm-slim

WORKDIR /app

COPY package.json package-lock.json tsconfig.json ./
COPY packages/protocol ./packages/protocol
COPY packages/server ./packages/server
COPY packages/sdk/package.json ./packages/sdk/package.json
COPY packages/dashboard/package.json ./packages/dashboard/package.json

RUN npm ci \
  && npx tsc -b --pretty false packages/server

ENV NODE_ENV=production
ENV PORT=8080
ENV RELAY_HOST=0.0.0.0

EXPOSE 8080

CMD ["node", "packages/server/dist/src/main.js"]
