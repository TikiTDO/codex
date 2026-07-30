mod artifact;
mod backend;
mod extension;
mod metadata;
mod tool;

pub use extension::install;

pub(crate) const IMAGE_GEN_NAMESPACE: &str = "image_gen";
pub(crate) const IMAGEGEN_TOOL_NAME: &str = "imagegen";
