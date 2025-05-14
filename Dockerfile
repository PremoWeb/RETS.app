ARG BUN_VERSION=1.2.13

# Builder stage
FROM oven/bun:${BUN_VERSION} AS builder

WORKDIR /app
ENV NODE_ENV=production

# Copy package files first
COPY --link bun.lock package.json ./

# Install dependencies
RUN bun install --ci

# Copy remaining files
COPY --link . .

# Build the service
RUN bun build service.ts --outdir dist --target bun

# Production stage
FROM oven/bun:${BUN_VERSION}

WORKDIR /app
COPY --chown=bun:bun --from=builder /app /app

# Create healthcheck script inline
RUN echo 'const check = async () => {\
    try {\
    const response = await fetch("http://127.0.0.1:3000/health", {\
    method: "HEAD",\
    timeout: 2000\
    });\
    process.exit(response.ok ? 0 : 1);\
    } catch (err) {\
    console.error("Health check failed:", err);\
    process.exit(1);\
    }\
    };\
    check();' > /app/healthcheck.js

ENV PORT=3000
ENV PHOTO_PORT=3000
EXPOSE 3000/tcp
USER bun

HEALTHCHECK --interval=10s --timeout=3s --start-period=2s --retries=3 \
    CMD bun run /app/healthcheck.js

CMD ["bun", "run", "/app/dist/service.js"]