FROM ekidd/rust-musl-builder:latest AS build

# Add source code
ADD --chown=rust:rust . ./

# Copy the source and build the application
RUN cargo install --target x86_64-unknown-linux-musl --path .

# Copy the statically linked binary into a alpine container
FROM alpine:latest
RUN apk add --no-cache ca-certificates
COPY --from=build /home/rust/.cargo/bin/nuker /usr/local/bin/
RUN adduser -D nuker
USER nuker
ENTRYPOINT ["/usr/local/bin/nuker"]