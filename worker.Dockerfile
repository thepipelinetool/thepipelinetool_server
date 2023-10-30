FROM rust:latest as server_builder

WORKDIR /app
COPY thepipelinetool thepipelinetool
COPY runner runner
COPY task task
COPY utils utils
WORKDIR /app/server
COPY server/src/dummy.rs .
COPY server/Cargo.toml .
RUN sed -i 's#bin/server.rs#dummy.rs#' Cargo.toml
RUN cargo install --path . --bin server

COPY server/Cargo.toml .
COPY server/src src
COPY server/bin bin
RUN cargo install --path . --bin worker

FROM rust:latest
WORKDIR /worker
COPY --from=server_builder /usr/local/cargo/bin/worker /usr/local/bin/worker

EXPOSE 8000

CMD worker
