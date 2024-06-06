use crate::Client;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock, RwLockWriteGuard},
};

impl Client {
    /// SSMの値をキャッシュできるようにします
    /// ```no_run
    /// # use ssm_utils::*;
    ///
    /// let client = Client::from_env().await.with_cache();
    /// client.get("aaa").await;
    /// client.get("aaa").await; // キャッシュされている
    /// ```
    pub fn with_cache(self) -> Client<EternalCache> {
        Client {
            ssm: self.ssm,
            cache: EternalCache::new_cache(),
        }
    }

    /// SSMの値をキャッシュできるようにします
    /// 時間経過で値が落ちるようになります。
    /// ```no_run
    /// # use ssm_utils::*;
    ///
    /// let client = Client::from_env().await.with_cache_expire(Duration::from_secs(60));
    /// client.get("aaa").await;
    /// client.get("aaa").await; // キャッシュされている
    /// // ... 最初の取得から一分後
    /// client.get("aaa").await; // キャッシュしなおす
    /// ```
    #[cfg(feature = "expire")]
    pub fn with_cache_expire(self, time_to_live: std::time::Duration) -> Client<ExpireCache> {
        Client {
            ssm: self.ssm,
            cache: ExpireCache::builder()
                .max_capacity(32)
                .time_to_live(time_to_live)
                .build(),
        }
    }

    /// キャッシュを追加します。
    pub fn with_cache_raw<C>(self, cache: C) -> Client<C> {
        Client {
            ssm: self.ssm,
            cache,
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

/// 永続キャッシュ
pub type EternalCache = Arc<RwLock<HashMap<String, String>>>;
/// 永続キャッシュ付きssm Client
pub type CachedClient = Client<EternalCache>;

impl Cache for EternalCache {
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

#[cfg(feature = "expire")]
pub type ExpireCache = mini_moka::sync::Cache<String, String>;
#[cfg(feature = "expire")]
pub type ExpireCachedClient = Client<ExpireCache>;

#[cfg(feature = "expire")]
impl Cache for ExpireCache {
    fn new_cache() -> Self
    where
        Self: Sized,
    {
        Self::builder()
            .max_capacity(32)
            .time_to_live(std::time::Duration::from_secs(60))
            .build()
    }

    fn get(&self, key: &str) -> Option<String> {
        self.get(&key.to_owned())
    }

    fn set(&self, key: &str, value: &str) {
        self.insert(key.into(), value.into())
    }
}
