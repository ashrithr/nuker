#[macro_export]
macro_rules! handle_future {
    ($request:expr) => {
        match $request.await.map_err(|err| $crate::Error::from(err)) {
            Ok(_result) => {
                tracing::trace!("Successfully executed request");
            }
            Err(err) => {
                tracing::error!(err = ?err, "Failed to execute request");
            }
        }
    };
}

#[macro_export]
macro_rules! handle_future_with_return {
    ($request:expr) => {
        match $request.await.map_err(|err| $crate::Error::from(err)) {
            Ok(result) => {
                tracing::trace!(result = crate::print_type_of(&result).as_str(), "Successfully executed request");
                Ok(result)
            }
            Err(err) => {
                tracing::error!(err = ?err, "Failed to execute request");
                Err(err)
            }
        }
    };
}

#[macro_export]
macro_rules! scan_resources {
    ($resource_type:expr, $resources:expr, $handles:expr, $service:expr, $region:expr) => {
        $handles.push(tokio::spawn(async move {
            match $service
                .scan()
                .instrument(tracing::trace_span!(
                    $resource_type,
                    region = $region.as_str()
                ))
                .await
            {
                Ok(rs) => {
                    if !rs.is_empty() {
                        for r in rs {
                            $resources.lock().unwrap().push(r);
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("Error occurred locating resources: {}", err);
                }
            }
        }));
    };
}

/// Constructs an `Error` using the standard string interpolation syntax.
#[macro_export]
macro_rules! format_err {
    ($($arg:tt)*) => { $crate::Error::from(format!($($arg)*)) }
}
