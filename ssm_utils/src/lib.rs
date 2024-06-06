use std::{
    collections::HashMap,
    sync::{Arc, RwLock, RwLockWriteGuard},
};

pub mod sdk {
    pub use aws_sdk_ssm::*;
}

pub mod sdk_config {
    pub use aws_config::*;
}

/// SSMのClient
///
/// キャッシュや、環境変数でMockさせることができます。
#[derive(Debug, Clone)]
pub struct Client<C = ()> {
    ssm: Option<sdk::Client>,
    cache: C,
}

impl Client {
    /// [`sdk::Client`]から[`Client`]を作ります
    pub fn from_ssm_client(ssm: sdk::Client) -> Self {
        Self {
            ssm: Some(ssm),
            cache: (),
        }
    }

    /// 環境変数から作ります
    pub async fn from_env() -> Self {
        let config = aws_config::from_env().load().await;
        Client::from_conf(&config)
    }

    /// [`sdk::Config`]から作ります
    /// [`sdk_config::SdkConfig`]なども受け入れられます。
    pub fn from_conf<C: Into<sdk::Config>>(conf: C) -> Self {
        Self::from_ssm_client(sdk::Client::from_conf(conf.into()))
    }

    /// SSMの値をキャッシュできるようにします
    /// ```no_run
    /// # use ssm_utils::*;
    ///
    /// let client = Client::from_env().await.with_cache();
    /// client.get("aaa").await;
    /// client.get("aaa").await; // キャッシュされている
    /// ```
    pub fn with_cache(self) -> Client<RwCache> {
        Client {
            ssm: self.ssm,
            cache: RwCache::new_cache(),
        }
    }

    /// Mock用のClientを作ります。
    /// このモードでは、環境変数の値から確認するようになります。
    pub fn mock() -> Client {
        Client {
            ssm: None,
            cache: (),
        }
    }

    /// Mock用のClientを作ります。
    /// このモードでは、mapの中身を確認し、その後環境変数を確認します。
    pub fn mock_from_map(map: HashMap<String, String>) -> CachedClient {
        Client {
            ssm: None,
            cache: Arc::new(RwLock::new(map)),
        }
    }
}

impl<C: Cache> Client<C> {
    /// `key`にあたる値をSSMから取得します。
    /// キャッシュが有効ならキャッシュを先に確認します。
    pub async fn get(&self, key: &str) -> Result<String, Error> {
        // キャッシュを見る
        if let Some(cached) = self.cache.get(key) {
            return Ok(cached);
        }
        // mockか確認
        let Some(ssm_client) = &self.ssm else {
            // mockならenvの値も確認する
            return std::env::var(key).map_err(|_| Error::NotFound);
        };
        // ssmに問い合わせる
        let resp = ssm_client
            .get_parameter()
            .name(key)
            .set_with_decryption(Some(true))
            .send()
            .await
            .map_err(|e| {
                // Not found かどうか
                if e.as_service_error()
                    .map(|e| e.is_parameter_not_found())
                    .unwrap_or(false)
                {
                    Error::NotFound
                } else {
                    Error::Ssm(e.into())
                }
            })?;
        match resp.parameter.and_then(|it| it.value) {
            Some(ok) => {
                // cacheがあればそこに入れる、ないとnoop
                self.cache.set(key, &ok);
                Ok(ok)
            }
            None => Err(Error::NotFound),
        }
    }

    /// [`sdk::Client`]を取得します。
    /// mockだとpanicします。
    pub fn raw_client(&self) -> &sdk::Client {
        self.ssm
            .as_ref()
            .expect("raw_client not supported in mock mode.")
    }

    /// mockかどうか。
    pub fn is_mock(&self) -> bool {
        self.ssm.is_none()
    }
}

/// キャッシュを規定する
pub trait Cache: Clone {
    fn new_cache() -> Self
    where
        Self: Sized;
    fn get(&self, key: &str) -> Option<String>;
    fn set(&self, key: &str, value: &str);
}

/// キャッシュしない
impl Cache for () {
    fn new_cache() -> Self {}
    /// 必ずNone
    #[inline]
    fn get(&self, _key: &str) -> Option<String> {
        None
    }
    /// noop
    #[inline]
    fn set(&self, _key: &str, _value: &str) {}
}

pub type RwCache = Arc<RwLock<HashMap<String, String>>>;
/// キャッシュ付きssm Client
pub type CachedClient = Client<RwCache>;

impl Cache for RwCache {
    fn new_cache() -> Self {
        Arc::new(RwLock::new(HashMap::new()))
    }
    /// キャッシュから取得
    /// Readのロックがかかるので、ほかにwriteのロックを書けてると待機します。
    fn get(&self, key: &str) -> Option<String> {
        self.as_ref()
            .read()
            .ok()
            .and_then(|rg| rg.get(key).cloned())
    }
    fn set(&self, key: &str, value: &str) {
        if let Ok(mut map) = self.write() {
            map.insert(key.to_owned(), value.to_owned());
        }
    }
}

impl CachedClient {
    /// キャッシュを取得する
    /// これがdropされないと[`Client::get`]が待機します。
    pub fn get_mut_cache(&self) -> Option<RwLockWriteGuard<HashMap<String, String>>> {
        self.cache.write().ok()
    }

    /// キャッシュを消す
    pub fn clear_cache(&self) {
        self.cache.write().expect("poisoned lock").clear();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Key not found")]
    NotFound,
    #[error(transparent)]
    Ssm(sdk::Error),
}
