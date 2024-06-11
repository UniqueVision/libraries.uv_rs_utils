# uv_rs_utils

社内用 Rust のライブラリ。
一応 Public にしているので、社外にでても問題ないものしか書かない。

# dynamodb_utils

[`aws_sdk_dynamodb`](`https://docs.rs/aws-sdk-dynamodb/latest/aws_sdk_dynamodb/`)の ラッパー。

特によく使う、get, put, delete item などが用意されている。
これらは`serde`から構造体を直接入れれるようにしている。
また特定の値を atomic に変更、加算する機能も用意。

# s3_utils

[`aws_sdk_s3`](`https://docs.rs/aws-sdk-s3/latest/aws_sdk_s3/`) のラッパー。

GET,PUT,LIST,DELETE などが楽にできるようにしてある。

# ssm_utils

[`aws_sdk_ssm`](`https://docs.rs/aws-sdk-ssm/latest/aws_sdk_ssm/`) のラッパー。

基本的に SSM の値を読み込むことのみに注力している。
取得した値を、永続的にまたは一時的にキャッシュする機能と、
環境変数で Mock するモードを用意してある。
