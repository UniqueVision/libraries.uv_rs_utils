pub use crate::cache::CachedClient;
use cache::Cache;

pub mod cache;
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

    /// Mock用のClientを作ります。
    /// このモードでは、環境変数の値から確認するようになります。
    pub fn mock() -> Client {
        Client {
            ssm: None,
            cache: (),
        }
    }
}

impl<C: Cache> Client<C> {
    /// `key`にあたる値をSSMから取得します。
    /// キャッシュが有効ならキャッシュを先に確認します。
    ///
    /// ### mockのとき
    /// キャッシュが有効ならキャッシュから確認し、
    /// そうでないなら環境変数から取得します。
    /// 環境変数は`/`、`-`を`_`に変換して大文字化したものも見るようにします。
    pub async fn get(&self, key: &str) -> Result<String, Error> {
        // キャッシュを見る
        if let Some(cached) = self.cache.get(key) {
            return Ok(cached);
        }
        // mockか確認
        let Some(ssm_client) = &self.ssm else {
            // mockならenvの値も確認する
            return std::env::var(key)
                .or_else(|_| std::env::var(key.replace("/", "_").replace("-", "_").to_uppercase()))
                .map_err(|_| Error::NotFound);
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
                    Error::Ssm(Box::new(e.into()))
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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Key not found")]
    NotFound,
    #[error(transparent)]
    Ssm(Box<sdk::Error>),
}
