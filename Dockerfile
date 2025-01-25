# syntax=docker/dockerfile:1.7.0-labs
ARG TAG=24.04

FROM ubuntu:${TAG} AS build

### Build stage

# Install curl and git and simplexmq dependencies
RUN apt-get update && apt-get install -y curl git build-essential libgmp3-dev zlib1g-dev llvm-18 llvm-18-dev libnuma-dev libssl-dev

# Specify bootstrap Haskell versions
ENV BOOTSTRAP_HASKELL_GHC_VERSION=9.6.3
ENV BOOTSTRAP_HASKELL_CABAL_VERSION=3.12.1.0

# Do not install Stack
ENV BOOTSTRAP_HASKELL_INSTALL_NO_STACK=true
ENV BOOTSTRAP_HASKELL_INSTALL_NO_STACK_HOOK=true

# Install ghcup
RUN curl --proto '=https' --tlsv1.2 -sSf https://get-ghcup.haskell.org | BOOTSTRAP_HASKELL_NONINTERACTIVE=1 sh

# Adjust PATH
ENV PATH="/root/.cabal/bin:/root/.ghcup/bin:$PATH"

# Set both as default
RUN ghcup set ghc "${BOOTSTRAP_HASKELL_GHC_VERSION}" && \
    ghcup set cabal "${BOOTSTRAP_HASKELL_CABAL_VERSION}"

# Copy only the source code
COPY apps /project/apps/
COPY cbits /project/cbits/
COPY src /project/src/

COPY cabal.project Setup.hs simplexmq.cabal LICENSE /project

WORKDIR /project

# Debug
#ARG CACHEBUST=1

#ADD --chmod=755 https://github.com/MShekow/directory-checksum/releases/download/v1.4.6/directory-checksum_1.4.6_linux_amd64 /usr/local/bin/directory-checksum
#RUN directory-checksum --max-depth 2 .

# Set build arguments and check if they exist
ARG APP
RUN if [ -z "$APP" ]; then printf "Please spcify \$APP build-arg.\n"; exit 1; fi

# Compile app
RUN cabal update
RUN cabal build exe:$APP

# Copy scripts
COPY scripts /project/scripts/

# Create new path containing all files needed
RUN mkdir /final
WORKDIR /final

# Strip the binary from debug symbols to reduce size
RUN bin="$(find /project/dist-newstyle -name "$APP" -type f -executable)" && \
    mv "$bin" ./ && \
    strip ./"$APP" &&\
    mv /project/scripts/docker/entrypoint-"$APP" ./entrypoint &&\
    mv /project/scripts/main/simplex-servers-stopscript ./simplex-servers-stopscript

### Final stage
FROM ubuntu:${TAG}

# Install OpenSSL dependency
RUN apt-get update && apt-get install -y openssl libnuma-dev

# Copy compiled app from build stage
COPY --from=build /final /usr/local/bin/

# Open app listening port
ARG APP_PORT
RUN if [ -z "$APP_PORT" ]; then printf "Please spcify \$APP_PORT build-arg.\n"; exit 1; fi

EXPOSE $APP_PORT

# simplexmq requires using SIGINT to correctly preserve undelivered messages and restore them on restart
STOPSIGNAL SIGINT

# Finally, execute helper script
ENTRYPOINT [ "/usr/local/bin/entrypoint" ]
