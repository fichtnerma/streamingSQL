###################
# BUILD FOR LOCAL DEVELOPMENT
###################

FROM node:18-alpine

ENV DATABASE_URL $DATABASE_URL
ENV NODE_ENV development

WORKDIR /app

COPY . /app

RUN apk update && apk upgrade
RUN apk add --no-cache curl

COPY --chown=node:node package*.json .
RUN npm ci && npm cache clean --force && npm prune
COPY --chown=node:node . .
RUN npx prisma generate
RUN npm run build

CMD [ "npx", "prisma", "migrate", "dev" ]
EXPOSE 3000
