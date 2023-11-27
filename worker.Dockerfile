FROM rust:latest as server_builder
WORKDIR /

ARG VERSION

RUN git clone --depth=1 --branch ${VERSION} https://github.com/thepipelinetool/thepipelinetool.git

WORKDIR /app
COPY server/src/dummy.rs .
COPY server/Cargo.toml .
RUN sed -i 's#bin/worker.rs#dummy.rs#' Cargo.toml
RUN cargo install --path . --bin worker

COPY server/Cargo.toml .
RUN cargo update
COPY server/src src
COPY server/bin bin
RUN cargo install --path . --bin worker

FROM rust:latest
WORKDIR /worker
COPY --from=server_builder /usr/local/cargo/bin/worker /usr/local/bin/worker

CMD worker
