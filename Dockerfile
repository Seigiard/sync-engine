FROM oven/bun:1.4.2-alpine
RUN apk add --no-cache util-linux
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY src ./src
COPY test ./test
COPY tsconfig.json ./
CMD ["bun", "run", "check"]
