FROM rust:1.41.0 AS build
WORKDIR /usr/src

# Install musl
RUN apt update -y && apt install -y musl

# Download the target for static linking
RUN rustup target add x86_64-unknown-linux-musl

# Create a empty project and build the app's dependencies
# If the Cargo.toml or Cargo.lock files have not changed,
# the docker build cache is used and skip these time consuming
# steps.
RUN USER=root cargo new aws-nuke
WORKDIR /usr/src/aws-nuke
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release

# Copy the source and build the application
COPY src ./src
RUN cargo install --target x86_64-unknown-linux-musl --path .

# Copy the statically linked binary into a scratch container
FROM scratch
COPY --from=build /usr/local/cargo/bin/aws-nuke .
USER 1000
CMD ["./aws-nuke"]