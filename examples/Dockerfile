FROM ghcr.io/thepipelinetool/thepipelinetool-server-dev:latest

WORKDIR /app

RUN git clone https://github.com/thepipelinetool/thepipelinetool.git

WORKDIR /app/thepipelinetool

RUN cargo install --path thepipelinetool --examples --root . --force --no-track

WORKDIR /app/thepipelinetool/dags

RUN cp -r /app/thepipelinetool/bin/* .

WORKDIR /app/thepipelinetool