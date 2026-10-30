# Build the independent worker and ship only compiled source and runtime dependencies.
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
ENV NODE_ENV=production
WORKDIR /app
COPY --from=production-dependencies /app/package.json ./package.json
COPY --from=production-dependencies /app/node_modules ./node_modules
COPY --from=build /app/dist/src ./dist/src
USER node
STOPSIGNAL SIGTERM
# Run Node directly so SIGTERM reaches the worker's bounded shutdown handler.
CMD ["node", "dist/src/main.js"]
