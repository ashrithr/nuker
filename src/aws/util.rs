use crate::{config::RequiredTags, resource::NTag};
use chrono::prelude::*;
use std::collections::HashMap;
use std::time::Duration;
use tracing::debug;

enum DtFormat<'a> {
    RFC2822,
    RFC3339,
    FromDtStr(&'a str),
    FromNdtStr(&'a str),
}

/// Compares resource tags against required tags
pub fn compare_tags(tags: Option<Vec<NTag>>, required_tags: &Vec<RequiredTags>) -> bool {
    let mut tags_map = HashMap::new();
    if tags.is_some() {
        for tag in tags.unwrap() {
            tags_map.insert(tag.key.unwrap(), tag.value.unwrap());
        }
    }

    for rt in required_tags {
        if tags_map.contains_key(&rt.name) {
            if rt.regex.is_some() {
                if !rt
                    .regex
                    .as_ref()
                    .unwrap()
                    .is_match(tags_map.get(&rt.name).unwrap())
                {
                    debug!(
                        "Required tag pattern does not match: {:?}",
                        tags_map.get(&rt.name)
                    );
                    return false;
                }
            }
        } else {
            debug!("Required tag ({}) is missing", rt.name);
            return false;
        }
    }

    true
}

/// Compares a given date to a specified duration to check if the date is older
pub fn is_ts_older_than(date: &str, older_than: &Duration) -> bool {
    let mut millis: Option<i64> = None;
    let try_formats = [
        DtFormat::RFC2822,
        DtFormat::RFC3339,
        DtFormat::FromDtStr("%+"),
        DtFormat::FromNdtStr("%Y-%m-%d %H:%M:%S GMT"),
    ]
    .iter();

    for try_format in try_formats {
        if millis.is_some() {
            break;
        }
        match try_format {
            &DtFormat::RFC2822 => {
                if let Ok(dt) = DateTime::parse_from_rfc2822(date) {
                    millis = Some(dt.timestamp_millis());
                }
            }
            &DtFormat::RFC3339 => {
                if let Ok(dt) = DateTime::parse_from_rfc3339(date) {
                    millis = Some(dt.timestamp_millis());
                }
            }
            &DtFormat::FromDtStr(ref fmt) => {
                if let Ok(dt) = DateTime::parse_from_str(date, fmt) {
                    millis = Some(dt.timestamp_millis());
                }
            }
            &DtFormat::FromNdtStr(ref fmt) => {
                if let Ok(dt) = NaiveDateTime::parse_from_str(date, fmt) {
                    millis = Some(dt.timestamp_millis());
                }
            }
        }
    }

    let start = Utc::now().timestamp_millis() - older_than.as_millis() as i64;

    if start > millis.unwrap_or(Utc::now().timestamp_millis()) {
        true
    } else {
        false
    }
}
