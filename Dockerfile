FROM ekidd/rust-musl-builder:latest AS build

# Add source code
ADD --chown=rust:rust . ./

# Copy the source and build the application
RUN cargo install --target x86_64-unknown-linux-musl --path .

# Copy the statically linked binary into a scratch container
FROM scratch
COPY --from=build /home/rust/.cargo/bin/aws-nuke .
USER 1000
ENTRYPOINT ["./aws-nuke"]