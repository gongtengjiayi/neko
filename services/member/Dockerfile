# syntax=docker/dockerfile:1.6
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .

FROM node:20-alpine
RUN apk add --no-cache curl
WORKDIR /app
COPY --from=build /app /app
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
  CMD curl -f http://localhost:8080/healthz || exit 1
CMD ["node", "src/index.js"]
