git clone https://github.com/thepipelinetool/thepipelinetool.git

cargo install --path thepipelinetool/thepipelinetool --examples --root . --force --no-track

cargo run --release --bin server