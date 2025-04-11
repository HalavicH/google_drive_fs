use derive_more::{Deref, From};
use error_stack::{ResultExt, bail};
use google_drive3::api::{File, Scope};
use google_drive3::common::Body;
use google_drive3::hyper_rustls::HttpsConnector;
use google_drive3::hyper_util::client::legacy::Client;
use google_drive3::hyper_util::client::legacy::connect::HttpConnector;
use google_drive3::yup_oauth2::authenticator::Authenticator;
use google_drive3::yup_oauth2::{ServiceAccountAuthenticator, read_service_account_key};
use google_drive3::{DriveHub, common, hyper_rustls, hyper_util};
use huh::{AMShared, IntoAMShared, IntoReport, maybe_extract_item};
use itertools::Itertools;
use mime_guess::Mime;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::Path;
use thiserror::Error;
use tracing::{debug, error, info};

pub type Result<T> = error_stack::Result<T, GoogleDriveFsError>;

#[derive(Debug, Error)]
pub enum GoogleDriveFsError {
    #[error("API request failed")]
    GoogleDriveApiError,
    #[error("FS error")]
    LocalFileError,
    #[error("Mime type is malformed")]
    MalformedMimeType,
    #[error("Multiple items with the same name")]
    MultipleItemsWithTheSameName,
    #[error("Source path '{0}' does not exists")]
    SourcePathDoesNotExists(String),
}

pub type GoogleDrive = DriveHub<HttpsConnector<HttpConnector>>;

// TODO: Implement global config
// pub struct Config {
//     conflict_behaviour: (),
//     cache_file_structure: (),
// }

pub struct GoogleDriveFs {
    pub hub: AMShared<GoogleDrive>,
    root_id: DriveItemId,
    // TODO: Implement caching of the file structure to improve latency
    // directory_structure: AMShared<HashMap<FilePath, DriveItemId>>, // FS path to Google Drive
}

pub const FOLDER_MIME_TYPE: &'static str = "application/vnd.google-apps.folder";
pub type FilePath = String;
#[derive(Debug, Clone, Deref, From)]
pub struct GdfsPath(String);
impl From<&str> for GdfsPath {
    fn from(value: &str) -> Self {
        value.to_owned().into()
    }
}
#[derive(Debug, Clone, Deref, From)]
pub struct DriveItemId(String /* TODO: Add file type */);
/// Instantiation
impl GoogleDriveFs {
    pub async fn new(root_id: DriveItemId, secret_json_path: &str) -> Result<GoogleDriveFs> {
        let auth = create_auth(secret_json_path).await;
        let client = create_client();
        let hub: DriveHub<HttpsConnector<HttpConnector>> = DriveHub::new(client, auth);

        let drive_fs = GoogleDriveFs {
            hub: hub.into_shared(),
            root_id,
            // directory_structure: Default::default(),
        };

        let root_name = "";

        let mapping = drive_fs.resolve_paths(&drive_fs.root_id, root_name).await?;
        info!("Resolved path: {:?}", mapping);
        Ok(drive_fs)
    }

    async fn resolve_paths(
        &self,
        root_id: &DriveItemId,
        root_name: &str,
    ) -> Result<HashMap<FilePath, DriveItemId>> {
        let folders = self.list_folders(&root_id).await?;

        let group_by = Self::group_by_name(&folders);

        let mapping: HashMap<FilePath, DriveItemId> = group_by
            .into_iter()
            .map(|(name, folders)| {
                if folders.len() > 1 {
                    error!(
                        "For parent {} found {} subfolders with the same name {}. Folders: {:#?}",
                        root_name,
                        folders.len(),
                        name,
                        folders
                    );
                }

                let path = format!("{}/{}", root_name, name);
                (
                    path,
                    folders[0]
                        .id
                        .as_ref()
                        .expect("Expected id to be present for folder")
                        .to_owned()
                        .into(),
                )
            })
            .collect();
        Ok(mapping)
    }
}

/// Low-level id based API
impl GoogleDriveFs {
    /// Create a folder on Drive and return its DriveFileId.
    pub async fn create_drive_folder(
        &self,
        folder_name: &str,
        parent_id: &DriveItemId,
    ) -> Result<DriveItemId> {
        let mut folder_metadata = File::default();
        folder_metadata.name = Some(folder_name.to_string());
        folder_metadata.parents = Some(vec![parent_id.deref().clone()]);
        folder_metadata.mime_type = Some(FOLDER_MIME_TYPE.to_string());

        let dummy = std::fs::File::open("/dev/null").expect("Expected to have /dev/null");
        let mime = FOLDER_MIME_TYPE.parse().expect("Expected to parse meta");
        let result = self
            .hub
            .lock()
            .await
            .files()
            .create(folder_metadata)
            .upload(&dummy, mime)
            .await
            .into_report()
            .change_context(GoogleDriveFsError::GoogleDriveApiError)
            .attach_printable_lazy(|| format!("Failed to create drive folder: {}", folder_name))?
            .1;

        Ok(result
            .id
            .expect("Expected id to be present for drive folder")
            .into())
    }

    pub async fn list_folders(&self, root_id: &DriveItemId) -> Result<Vec<File>> {
        let files = self.list_children(&root_id).await?;

        let folders = files
            .into_iter()
            .filter(|f| f.mime_type == Some(FOLDER_MIME_TYPE.to_string()))
            .collect::<Vec<_>>();
        Ok(folders)
    }

    pub async fn list_children(&self, parent_id: &DriveItemId) -> Result<Vec<File>> {
        let file_list = self
            .hub
            .lock()
            .await
            .files()
            .list()
            .add_scope(Scope::Full)
            .include_items_from_all_drives(true)
            .supports_all_drives(true)
            .q(&format!("'{}' in parents", parent_id.deref()))
            .doit()
            .await
            .change_context(GoogleDriveFsError::GoogleDriveApiError)
            .attach_printable_lazy(|| format!("Parent id: {}", parent_id.deref()))?
            .1;

        if let Some(_token) = file_list.next_page_token {
            todo!("Fetch additional data before processing")
        }

        Ok(file_list.files.unwrap_or_default())
    }
    pub async fn get_file_id_by_name_and_parent(
        &self,
        parent_id: &DriveItemId,
        name: &str,
    ) -> Result<Option<DriveItemId>> {
        let vec: Vec<File> = self.list_children(&parent_id).await?;
        let name_matches = vec
            .into_iter()
            .filter(|f| f.name == Some(name.to_string()))
            .collect::<Vec<File>>();

        let option = maybe_extract_item(name_matches, "name")
            .change_context(GoogleDriveFsError::MultipleItemsWithTheSameName)?;
        Ok(option.map(|f| f.id.expect("Expected drive item to have id").into()))
    }
}

/// FS based API (no name collisions allowed)
impl GoogleDriveFs {
    /// Creates directories recursively (like -p is used)
    pub async fn mkdir(&self, path: &GdfsPath) -> Result<DriveItemId> {
        let dir_names = path
            .split("/")
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        debug!(
            "For input path: {:?} got produced parts: {:?}",
            path, dir_names
        );

        let mut parent_id = self.root_id.clone();
        // Traverse path creating directories
        for name in dir_names {
            debug!("Traversing '{}'", name);
            let maybe_parent = self
                .get_file_id_by_name_and_parent(&parent_id, name)
                .await?;
            parent_id = match maybe_parent {
                None => {
                    info!("Directory '{}' is not yet present. Creating it", name);
                    self.create_drive_folder(name, &parent_id).await?
                }
                Some(p) => {
                    debug!(
                        "Directory '{}' is already present. Continue traversing",
                        name
                    );
                    p
                }
            }
        }

        Ok(parent_id)
    }
    /// Copies a local file to Google Drive.
    /// Fails if it finds 2 directories with the same name
    pub async fn cp(&self, src_local_path: &str, dst_gdfs_path: &GdfsPath) -> Result<DriveItemId> {
        // 1. Check if local file exists.
        let src_path = Path::new(src_local_path);
        if !src_path.exists() {
            bail!(GoogleDriveFsError::SourcePathDoesNotExists(
                src_local_path.to_string()
            ));
        }

        // 2. Check if destination folder exists; if not, create it.
        let dst_path = Path::new(dst_gdfs_path.deref());
        let dst_directories_path: GdfsPath = match dst_path.parent() {
            None => {
                debug!("Dst path '{:?}' has root parent???", dst_path);
                todo!("Verify this case branch");
            }
            Some(p) => p.to_str().expect("Expected to convert path to str").into(),
        };
        let parent_folder_id = self.mkdir(&dst_directories_path).await?;

        debug!(
            "Got parent folder id: {:?} for path {:?}",
            parent_folder_id, dst_directories_path
        );

        // 3. Get the local file name from the local path.
        let file_name = dst_path
            .file_name()
            .expect("Expected local path to have file name")
            .to_str()
            .expect("Expected file name to be valid UTF-8");

        // 4. Read the file contents and determine its MIME type.
        let fs_file = std::fs::File::open(&src_local_path)
            .change_context(GoogleDriveFsError::LocalFileError)
            .attach_printable_lazy(|| format!("Local file: {}", src_local_path))?;

        // Use mime_guess crate to infer MIME type.
        let mime_type = mime_guess::from_path(&src_local_path)
            .first_or_octet_stream()
            .to_string();

        // 5. Create a Drive file metadata record.
        let mut drive_file = File::default();
        drive_file.name = Some(file_name.to_string());
        drive_file.parents = Some(vec![parent_folder_id.deref().clone()]);
        // drive_file.mime_type = Some(mime_type);

        debug!(
            "Going to perform create file request with:\
               src_path {:?}, dst_path {:?}, mime_type: {:?}",
            src_path, dst_path, mime_type
        );
        // 6. Upload the file by invoking the Drive API.
        let mime: Mime = mime_type
            .parse::<Mime>()
            .change_context(GoogleDriveFsError::MalformedMimeType)?;
        let result = self
            .hub
            .lock()
            .await
            .files()
            .create(drive_file)
            .upload(fs_file, mime)
            .await
            .change_context(GoogleDriveFsError::GoogleDriveApiError)
            .attach_printable_lazy(|| {
                format!(
                    "Local file path: {:?}. Gdfs file path {:?}",
                    src_path, dst_path
                )
            })?;

        let file_id = result
            .1
            .id
            .expect("Expected id to be present for drive file");
        info!(
            "Successfully uploaded local file '{}' as Drive file with id {}",
            src_local_path, file_id
        );

        Ok(file_id.into())
    }

    fn group_by_name(folders: &Vec<File>) -> HashMap<String, Vec<&File>> {
        folders
            .into_iter()
            .sorted_by_key(|f| f.name.clone().unwrap_or_else(|| "Unknown".to_string()))
            .chunk_by(|&f| f.name.clone().unwrap_or("Unknown".to_string()))
            .into_iter()
            .map(|(key, group)| (key, group.collect()))
            .collect()
    }

    pub fn create_folder(&self, _folder_name: &str) -> common::Result<()> {
        todo!()
        // let file = File::default();
        // self.hub.files()
        //     .create(file)
        //     .upload()
    }

    pub fn create_file(&self, _file_path: &str) -> common::Result<File> {
        todo!()
        // let file = File::default();
        // self.hub.files()
        //     .create(file)
        //     .upload()
    }
}

fn create_client() -> Client<HttpsConnector<HttpConnector>, Body> {
    // Create a new HTTPS connector
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .expect("Expected to create HTTPS connector builder")
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();

    let client = Client::builder(hyper_util::rt::TokioExecutor::new()).build(connector);
    client
}

async fn create_auth(path: &str) -> Authenticator<HttpsConnector<HttpConnector>> {
    // Load the service account key from a file
    let key = read_service_account_key(path)
        .await
        .expect("Expected to read service account key");

    // Create a new authenticator
    let auth = ServiceAccountAuthenticator::builder(key)
        .build()
        .await
        .expect("Expected to create authenticator");
    auth
}
