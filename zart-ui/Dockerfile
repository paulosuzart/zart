# Stage 1 — build the React SPA
FROM node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci

COPY index.html tsconfig.json vite.config.ts ./
COPY src ./src

RUN npm run build

# Stage 2 — serve with nginx
FROM nginx:alpine

ARG API_URL=http://host.docker.internal:3000
ENV API_URL=${API_URL}

COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

RUN sed -i 's|\${API_URL}|'"${API_URL}"'|g' /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
