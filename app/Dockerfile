FROM node:22-bookworm-slim
ENV NODE_ENV=production
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY backend ./backend
COPY public ./public
RUN mkdir -p /app/var && chown node:node /app/var
USER node
EXPOSE 3000
CMD ["node", "backend/server.mjs"]
