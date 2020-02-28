use crate::config::RequiredTags;
use crate::service::NTag;
use log::debug;
use std::collections::HashMap;

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
