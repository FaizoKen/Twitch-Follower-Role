FROM rust:1.88-bookworm AS builder
WORKDIR /app

# Cache dependencies in a separate layer
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/twitch-follower-role target/release/deps/twitch_follower_role*

# Build actual source
COPY favicon.ico ./
COPY src/ src/
COPY migrations/ migrations/
COPY templates/ templates/
RUN cargo build --release && strip target/release/twitch-follower-role

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/twitch-follower-role /usr/local/bin/
EXPOSE 8080
CMD ["twitch-follower-role"]
