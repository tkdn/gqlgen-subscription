# 実AWS環境への移行方針

(2) 完了キュー→Lambdaトリガーの検証にあたり、kumo（ローカルAWSエミュレーター）の実装状況を調査した結果を受けての方針転換メモ。[ブラッシュアップ3案の見立て](./20260712-brushup-feasibility.md)・[architecture.md](./architecture.md)を前提とする。

## 転換の経緯：kumoの制約

kumoのSQS→Lambda event source mappingは、コントロールプレーンAPI（`CreateEventSourceMapping`等のCRUD）のみ実装されており、データプレーン（SQSにメッセージが届いたら実際にLambdaを起動する処理）が実装されていないことが調査で判明した。

根拠:

- kumo PR [#202](https://github.com/sivchari/kumo/pull/202)「feat(lambda): add EventSourceMapping API support」（2026-02-06マージ）は`CreateEventSourceMapping`/`Get`/`Delete`/`List`/`Update`の5 APIのみを追加。統合テストも`CreateAndDelete`/`GetAndUpdate`のCRUD確認のみで、実際にSQSへメッセージを送りLambda起動を確認するテストが存在しない
- kumoの`internal/service/sqs/`配下のソースに`EventSourceMapping`への参照が一切ない
- 対照的にS3→Lambdaは直近のPR [#842](https://github.com/sivchari/kumo/pull/842)（2026-07-10マージ）で`internal/server/s3_lambda_wiring_test.go`という専用配線とテストが追加されている。SQSには同等のファイルが存在しない。同PR本文にも「S3 bucket notifications delivered to SQS ... but `LambdaFunctionConfigurations` were silently dropped ... and never invoked」とあり、S3でさえ直近まで未実装だった

この制約下でLambdaハンドラを書いても、「kumo経由でSQSトリガーとして実際に動く」ことをローカルで検証できない。これを受け、(2)の検証を実AWS環境で行う方針に転換する。

## 確定した方針

### 対象範囲

- **実AWS化するのはSQS・Lambda・PostgreSQL（RDS）**。app・postgresqlの同居構成が水平スケールと矛盾することが判明したため、PostgreSQLもRDSへ切り出す（後述「インフラ構成」参照）
- Redisは実AWS化の対象外。引き続きローカルDocker（`docker-compose.yml`）の参照実装用のみ

### インフラ構成

- ECSクラスター + サービス + タスク（**appのみ**。DBは同居させない）
  - 検討の過程で「app・postgresqlを同一タスク定義に同居させるサイドカー構成」も候補に挙がったが、タスク数（`desiredCount`）を増やすとpostgresqlコンテナも複製され、(1) データの正本が分裂する、(2) NOTIFY/LISTENが単一DBインスタンス前提で設計されている（[plan/20260713-postgres-notify-listen.md](../plan/20260713-postgres-notify-listen.md)参照）ため通知がタスクをまたいで届かなくなる、という2点でarchitecture.md §4・§9の設計と矛盾することが判明し却下した
- app用のDockerfileを新規作成、ECRで管理
- RDS for PostgreSQL（単一インスタンス、`db.t4g.micro`相当）。appのタスク定義から独立したリソースとして構築し、タスク数に依存しない
- SQSキュー（依頼・完了の2つ、現行のkumo版と同じ構成）
- Lambda（完了キューのevent source mappingで起動。(2)の本題）
- IaCはTerraformで管理

### コスト

- SQS・Lambdaはこの検証規模（手動でジョブを数件作る程度）なら無料枠内に収まる見込み
- RDSは、AWS公式の[Free Tierページ](https://aws.amazon.com/free/)によれば「新規AWSアカウントはRDS for PostgreSQLをAWS Free Tierの一部として無料で開始できる。1年間、シングルAZインスタンスの一部で月750時間、汎用SSDストレージ20GB、自動バックアップストレージ20GBまでを含む」とされている（対象インスタンスクラス・恒久無料か12ヶ月限定かの詳細は本メモ作成時点で一次情報から確認しきれておらず、実装時にAWSマネジメントコンソールまたは[料金計算ツール](https://calculator.aws/)で確認する）。この検証は短時間の利用想定のため、無料枠が適用されれば枠内に収まる見込み
- 無料枠が適用されない場合の実費は、実装時に[AWS Pricing Calculator](https://calculator.aws/)で対象リージョン・インスタンスクラス（`db.t4g.micro`等）を指定して確認する（本メモ作成時点で具体的な時間単価の一次ソースを確認できていないため、ここでは概算を明記しない）
- [AWS公式ドキュメント](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_StopInstance.html)によれば、DBインスタンスは停止しても最大7日間しか停止状態を維持できず、「7日間手動で起動しなかった場合、RDSが自動的にインスタンスを起動する（Automatic restart of a stopped DB instance）」仕様がある。停止中も、プロビジョニング済みストレージとバックアップストレージの課金は継続する。検証終了後はTerraformで確実に破棄する運用とする（`terraform destroy`をREADME等に明記する）

### ローカル検証との関係（変更しない部分）

- `docker-compose.yml`（postgres/redis/kumo）は無変更。既存のkumo経由のテスト（`go test ./...`、e2e含む）も一切変更しない
- 実AWSを使う検証は、通常の`go test ./...`には含めない**別経路**として新規追加する（ビルドタグ等で明示指定したときのみ実行される想定。具体的な実行方式は(2)の実装計画で詰める）
- appはローカルでは引き続き`go run ./cmd/main.go`のホストプロセスとして起動する。app自体のDockerize（Dockerfile作成）はECSデプロイに必要な範囲でのみ行い、ローカル開発フローは変更しない

## スコープ外（今回明示的に含めないこと）

- Redisの実AWS化（ElastiCache等）
- RDSのマルチAZ化・リードレプリカ等の高可用性構成（単一インスタンスのみ）
- ECS水平スケール時の実負荷検証（RDS切り離しにより構造的な矛盾は解消される想定だが、実際に複数タスクで負荷をかけての検証は行わない）
- app自体のフルコンテナ化を前提としたローカル開発フローの変更
- **ECSへのデプロイ手法**。IaC（Terraform）で管理するのはSQS/Lambda/RDS/ECSクラスター・サービス・タスク定義等のAWSリソースそのものであり、タスク定義の更新やデプロイの自動化手法（[ecspresso](https://github.com/kayac/ecspresso)を使う想定）は今回の計画には含めない。デプロイ手法は別途検討する

## 次のステップ

この方針を前提に、(2) Lambda化＋実AWS化（ECS/RDS/SQS/Lambda/Terraform）の実装計画を別途`plan/`に立てる。
