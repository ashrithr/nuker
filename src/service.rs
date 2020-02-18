/// Represents a Nukable service

type Result<T, E = crate::error::Error> = std::result::Result<T, E>;

#[derive(Display, Debug)]
pub enum ResourceType {
    EC2,
    Volume,
    RDS,
    Aurora,
    S3,
}

impl ResourceType {
    pub fn is_ec2(&self) -> bool {
        match *self {
            ResourceType::EC2 | ResourceType::Volume => true,
            _ => false,
        }
    }

    pub fn is_instance(&self) -> bool {
        match *self {
            ResourceType::EC2 => true,
            _ => false,
        }
    }

    pub fn is_volume(&self) -> bool {
        match *self {
            ResourceType::Volume => true,
            _ => false,
        }
    }

    pub fn is_rds(&self) -> bool {
        match *self {
            ResourceType::RDS => true,
            _ => false,
        }
    }

    pub fn is_aurora(&self) -> bool {
        match *self {
            ResourceType::Aurora => true,
            _ => false,
        }
    }

    pub fn is_s3(&self) -> bool {
        match *self {
            ResourceType::S3 => true,
            _ => false,
        }
    }
}

#[allow(dead_code)]
pub enum FilterRule {
    /// A filter rule that checks if the required tags are provided
    /// for a given resource
    RequiredTags,
    /// A filter rule that checks to see if the resource falls under
    /// Idle (no usage)
    Idle,
    /// A filter rule that checks to see if the resource is using
    /// allowed type of the resource
    AllowedTypes,
}

#[derive(Debug)]
pub struct NTag {
    pub key: Option<String>,
    pub value: Option<String>,
}

#[derive(Debug)]
pub struct Resource {
    pub id: String,
    pub resource_type: ResourceType,
    pub tags: Option<Vec<NTag>>,
    pub state: Option<String>,
}

pub trait NukeService: ::std::any::Any {
    /// Get all the resources without applying any filters
    fn scan(&self) -> Result<Vec<Resource>>;

    /// Clean up the resources
    fn cleanup(&self, resources: Vec<&Resource>) -> Result<()>;

    fn as_any(&self) -> &dyn ::std::any::Any;
}

pub trait RequiredTagsFilter {
    fn filter(&self) -> Result<Vec<Resource>>;
}

pub trait AllowedTypesFilter {
    fn filter(&self) -> Result<Vec<Resource>>;
}

pub trait IdleFilter {
    fn filter(&self) -> Result<Vec<Resource>>;
}
