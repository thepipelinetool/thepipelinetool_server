FROM rust:latest as server_builder
WORKDIR /
RUN git clone --depth=1 --branch v0.1.215 https://github.com/thepipelinetool/thepipelinetool.git

WORKDIR /app
COPY server/src/dummy.rs .
COPY server/Cargo.toml .
RUN sed -i 's#bin/server.rs#dummy.rs#' Cargo.toml
RUN cargo install --path . --bin server

COPY server/Cargo.toml .
RUN cargo update
COPY server/src src
COPY server/bin bin
RUN cargo install --path . --bin server

FROM rust:latest
WORKDIR /server

RUN curl -L -o web.zip https://github.com/thepipelinetool/thepipelinetool_ui/releases/download/2023-11-20/web.zip
RUN unzip web.zip -d temp
RUN mkdir static && mv temp/* static/
RUN rm -r temp
RUN rm web.zip

COPY --from=server_builder /usr/local/cargo/bin/server /usr/local/bin/server

EXPOSE 8000

CMD server
