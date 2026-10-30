# Build the independent API and ship only its compiled source and runtime dependencies.
FROM node:22.23.1-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
COPY test ./test
RUN npm run build

FROM node:22.23.1-alpine AS production-dependencies
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

FROM node:22.23.1-alpine AS runtime
ENV NODE_ENV=production HOST=0.0.0.0 PORT=43187
WORKDIR /app
COPY --from=production-dependencies /app/package.json ./package.json
COPY --from=production-dependencies /app/node_modules ./node_modules
COPY --from=build /app/dist/src ./dist/src
USER node
EXPOSE 43187
STOPSIGNAL SIGTERM
# Liveness stays healthy during dependency outages; admission readiness is a separate route.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["node", "--input-type=module", "-e", "try { const response = await fetch('http://127.0.0.1:' + process.env.PORT + '/health/live', { signal: AbortSignal.timeout(3000) }); process.exit(response.ok ? 0 : 1); } catch { process.exit(1); }"]
CMD ["node", "dist/src/main.js"]
