/// ssmのClient
#[derive(Debug)]
pub struct Client<C = ()> {
    ssm: aws_sdk_ssm::Client,
    cache: C,
}

impl Client {
    /// [`aws_sdk_ssm::Client`]から[`Client`]を作ります
    pub fn from_ssm_client(ssm: aws_sdk_ssm::Client) -> Self {
        Self { ssm, cache: () }
    }

    /// 環境変数から作ります
    pub async fn from_env() -> Self {
        let config = aws_config::from_env().load().await;
        Client::from_conf(&config)
    }

    /// コンフィグから作ります
    pub fn from_conf<C: Into<aws_sdk_ssm::Config>>(conf: C) -> Self {
        Self::from_ssm_client(aws_sdk_ssm::Client::from_conf(conf.into()))
    }
}

impl AsRef<aws_sdk_ssm::Client> for Client {
    fn as_ref(&self) -> &aws_sdk_ssm::Client {
        &self.ssm
    }
}

impl<C> Client<C> {
    /// `key`にあたる値をSSMから取得します。
    pub async fn get(&self, key: &str) -> Result<String, Error> {
        let resp = self
            .ssm
            .get_parameter()
            .name(key)
            .set_with_decryption(Some(true))
            .send()
            .await
            .map_err(|e| Error::Ssm(e.into()))?;
        resp.parameter()
            .and_then(|it| it.value())
            .map(|it| it.to_owned())
            .ok_or(Error::NotFound)
    }
}

#[cfg(feature = "cache")]
mod cache {
    use super::*;
    pub type Cache = quick_cache::sync::Cache<String, String>;

    impl Client {
        /// SSMの値の取得をキャッシュできるようにします。
        ///
        /// 内部でキャパシティーが32のキャッシュを作成します。
        pub fn with_cache(self) -> Client<Cache> {
            Client {
                ssm: self.ssm,
                cache: Cache::new(32),
            }
        }

        /// SSMの値の取得をキャッシュできるようにします。
        pub fn with_cache_raw(self, cache: Cache) -> Client<Cache> {
            Client {
                ssm: self.ssm,
                cache,
            }
        }
    }

    impl Client<Cache> {
        /// キャッシュの内容を取得します
        pub fn get_cache_store(&self) -> &Cache {
            &self.cache
        }

        /// 値を取得をSSMから取得します。
        /// 取得した値はキャッシュされます。
        pub async fn get_cached(&self, key: &str) -> Result<String, Error> {
            self.cache.get_or_insert_async(key, self.get(key)).await
        }

        /// キャッシュの値をクリアします。
        pub fn reset_cahce(&self) {
            self.cache.clear()
        }

        /// キャッシュの中に値を入れます
        /// テストなどでssmから実際に取得する代わりに使えます。
        pub fn insert_to_cache(&self, key: impl Into<String>, value: impl Into<String>) {
            self.cache.insert(key.into(), value.into())
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Ssm(aws_sdk_ssm::Error),
    #[error("Not found")]
    NotFound,
}
