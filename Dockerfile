FROM oven/bun:1 AS builder
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

FROM oven/bun:1-slim
WORKDIR /app
RUN addgroup --system nonroot && adduser --system --ingroup nonroot nonroot
COPY --from=builder /app/node_modules node_modules
COPY src/ src/
COPY tsconfig.json ./
USER nonroot
ENV NODE_ENV=production
EXPOSE 3000
CMD ["bun", "run", "--smol", "src/index.ts"]
