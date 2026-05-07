# Databricks notebook source
# MAGIC %md このノートブックは https://github.com/databricks-industry-solutions/smart-claims.git で公開されています

# COMMAND ----------

# MAGIC %md 
# MAGIC <img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fsi/fs-lakehouse-logo-transparent.png width="600px">
# MAGIC 
# MAGIC [![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
# MAGIC [![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
# MAGIC [![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)
# MAGIC 
# MAGIC * <b>ドメイン</b>: 保険
# MAGIC * <b>課題</b>:
# MAGIC   * 保険会社は競争に勝つために絶えず革新し続けなければならない
# MAGIC   * 顧客がより競争力のある保険料を常に探し回るため、顧客維持・ロイヤルティの確保は難しい課題となっている
# MAGIC   * 不正請求は利益率を圧迫する
# MAGIC   * 保険金請求の処理に非常に時間がかかる
# MAGIC   * <i>請求処理の迅速化、処理コストの削減、不正の早期検知を実現するために、保険金請求管理プロセスをどのように改善するか。</i>
# MAGIC * <b><span style="color:#f03c15"> ソリューション: Smart Claims（スマート・クレームス）！ </span></b>
# MAGIC   * Lakehouse パラダイムを活用し、人間の調査を支援するプロセスの一部を自動化する Databricks ソリューションアクセラレーター
# MAGIC   * 詳細はドキュメントの「はじめに」を参照してください
# MAGIC 
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/InsuranceReferenceArchitecture.png" width="70%" height="70%">
# MAGIC すべての請求は異なります。以下のステップは典型的なワークフローを示しています。<br>
# MAGIC 1. <b>被保険者</b>がポリシーに関する主要な窓口であるブローカーに連絡する<br>
# MAGIC 2. <b>ブローカー</b>はデータを確認し、請求状況の関連詳細が記録されているかを確認する<br>
# MAGIC <b>アジャスター（査定員）</b>が調査を引き継ぎ、保険証券によってカバーされる損失・損害額を判断するために社内外の専門家と協力することがある。<br>
# MAGIC  3. <b>請求調査員</b>が書類の精査を行う<br>
# MAGIC  4. <b>コンプライアンス担当者</b>が補償対象の適格性を確認し、不正行為がないかチェックする<br>
# MAGIC  5. <b>鑑定士</b>が損害評価を行い、請求の重大度を判断する<br>
# MAGIC 6. <b>アジャスター</b>が支払いの承認・実行を確認し、<b>被保険者</b>に結果を通知する<br>
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC # 詳細
# MAGIC * <b>課題（What）</b>
# MAGIC   * より低い保険料を提供しながら競争力を維持し、かつ収益性を保つために運営コストをどう管理するか？
# MAGIC   * 顧客ロイヤルティと維持率を向上させ、解約を減らすにはどうすればよいか？
# MAGIC   * 請求の状況・判断に関する顧客へのレスポンスタイムを短縮するためにプロセス効率をどう高めるか？
# MAGIC   * 正当な当事者に対してタイムリーに資金・リソースを提供するにはどうすればよいか？
# MAGIC   * 不審な活動をフラグ立てして追加調査につなげるにはどうすればよいか？
# MAGIC * <b>目的（Why）</b>
# MAGIC   * 迅速な承認は顧客 NPS スコアの向上と運営費の削減につながる
# MAGIC   * 不正シナリオの検出・防止はリーケージ比率の低下につながる
# MAGIC   * 顧客満足度の向上はロス比率の低下につながる
# MAGIC * <b>手段（How）：請求自動化</b>
# MAGIC   * 単調で予測可能なタスクにおける人員依存を減らすため、請求処理パイプラインの特定の側面を自動化する
# MAGIC   * 人間の調査を支援・促進するために既存の請求データに追加情報・インサイトを付与する（例：次のベストアクションの推薦）
# MAGIC   * より良い意思決定のために状況・案件の説明可能性を向上させる
# MAGIC   * 人的エラー・バイアスを防ぐサポートを行い、請求関連担当者に監査証跡を提供する
# MAGIC 
# MAGIC # 保険業界の新興トレンド
# MAGIC * EY によれば：「保険の将来がデータドリブンかつ分析主導になることは自明です。しかし明日のトップ保険会社は、人とのつながりを構築し、適切なタイミングで個人的な対応をすることにも優れているでしょう。」
# MAGIC * Deloitte の「2023年保険業界アウトルック」では「テクノロジーインフラは改善されたが、焦点を価値の実現にシフトし、リスクとコスト削減という従来の重点から、継続的なイノベーション・競争差別化・収益性ある成長を促す実験と挑戦の拡大へと優先順位を広げる必要がある」と述べている。
# MAGIC * Nationwide のCTO、Jim Fowler氏は「保険の未来」に関するポッドキャストでイノベーションを中心に語っている。
# MAGIC * 個々のニーズは異なる。そのため、パーソナライゼーションと関連する価値を個人に提供することがイノベーションの重要な要素である。
# MAGIC * 勇気と確信に加え、イノベーションには忍耐が必要である。価値ある変化は一夜にして実現しないからだ。そのため、革新的なアイデアの実行においてテクノロジーが制約や障害とならないよう、高速なイノベーションを可能にするプラットフォームとオープン・拡張可能・プラグイン可能なアーキテクチャが求められる。
# MAGIC 
# MAGIC # 保険用語
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/Insurance Technology.png" width="70%" height="70%">
# MAGIC 
# MAGIC # 保険リファレンスアーキテクチャ
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/InsuranceReferenceArchitecture.png" width="70%" height="70%">
# MAGIC 
# MAGIC # Smart Claims リファレンスアーキテクチャとデータフロー
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/smart_claims_process.png" width="70%" height="70%">
# MAGIC 
# MAGIC 上図に示すように、請求フローは通常 Guidewire などの<b>オペレーショナル</b>システムと Databricks などの<b>分析</b>システムとの間でオーケストレーションを伴う。エンドユーザーはスマートアプリを使って請求を申請したり、ケースのステータスを確認したりすることが多い。アプリまたは車両に組み込まれた IoT デバイスを通じて、テレマティクスデータが常時これらのシステムにストリーミングされており、運転パターンに関する豊富な情報を提供する。他の信用スコアと組み合わせて、このデータはドライバーのリスクスコアの算出に使用され、保険料に直接影響する。ある意味で、この種の<b>保険リスクスコア</b>は、主に財務履歴から算出される一般的な財務信用スコアよりも、その人の安全運転記録をより適切に示す指標といえる。
# MAGIC 
# MAGIC 1. ポリシーデータの取り込み
# MAGIC 2. 請求・テレマティクスデータの取り込み
# MAGIC 3. すべてのデータソースをクラウドストレージに取り込む
# MAGIC 4. 生データを差分 Bronze テーブルに逐次ロード
# MAGIC 5. データの変換・操作
# MAGIC 6. モデルスコアリング（トレーニングパイプラインではモデルトレーニングも含む）
# MAGIC 7. 予測結果をゴールドテーブルにロードして集計を実行
# MAGIC 8. ダッシュボードによる可視化
# MAGIC 9. 結果をオペレーショナルシステムにフィードバック
# MAGIC 10. 判断に基づく請求ルーティング
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC # データセット
# MAGIC * すべてのデータは画像・地理座標を含む合成データである
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/datasets.png" width="60%" height="60%">
# MAGIC 
# MAGIC * 代表的なデータセットには上記のものが含まれ、移動が遅いものもあれば速いものもある。
# MAGIC * 構造化・半構造化のものもあれば、非構造化のものもある。
# MAGIC * 追記で蓄積されるものもあれば、差分更新でゆっくり変化するディメンション（SCD）として扱われるものもある。
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC # ドメインモデル
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/domain_model.png">
# MAGIC 
# MAGIC * 業界標準のデータドメインモデルが複数存在する（例：OMG https://www.omg.org/）
# MAGIC * 上の図はこのユースケースに関連するデータポイントを整理した簡略化されたドメインモデルである。
# MAGIC * 詳細は P&C エンティティ定義・用語・論理モデルを参照 https://www.omg.org/spec/PC/1.0/PDF
# MAGIC 
# MAGIC # ML とルールエンジンによるインサイト生成
# MAGIC * 事前学習済みの<b>ML モデル</b>を使用して、請求レコードに添付された画像をスコアリングし、損害の重大度を評価する。
# MAGIC * <b>ルールエンジン</b>は、人間を介さずに適用可能な既知の運用上の静的チェックを定義する柔軟な方法であり、「日常的なケース」の処理を迅速化する。報告データが自動検出情報と一致しない場合、追加の人間による調査を促すフラグが立てられる。
# MAGIC * この追加情報は、請求調査員が介入が必要なケース数を絞り込み、追加のフォローアップや精査が必要な領域を特定するのに役立つ。
# MAGIC * 主なチェック項目には以下が含まれる：
# MAGIC   * 請求日が補償期間内であること
# MAGIC   * 報告された重大度が ML の予測重大度と一致すること
# MAGIC   * テレマティクスデータで報告された事故場所が請求書に記載の場所と一致すること
# MAGIC   * テレマティクスで報告された速度が、責任の所在に争いがある場合に該当地域の制限速度内であること
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/rule_engine.png" width="70%" height="70%">
# MAGIC 
# MAGIC # ワークフロー
# MAGIC * 異なるデータソースはそれぞれのペースで流入し、独立したものもあれば依存関係があるものもある
# MAGIC * Databricks のマルチタスク ワークフローを使用してプロセスを自動化し、Lakehouse パラダイムを実証する。
# MAGIC * 一部のノードはメダリオンアーキテクチャを採用した Delta Live Table ノードであり、その他はモデルでデータをスコアリングするノートブック、または新たなインサイトでダッシュボードを更新する SQL ワークフローである。
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/workflow.png" width="60%" height="60%">
# MAGIC 1. セットアップ：すべての準備作業を実施<br>
# MAGIC 2. DLT パイプラインを使用して請求・ポリシー・事故データを取り込む<br>
# MAGIC 3. テレマティクスデータを取り込む<br>
# MAGIC 4. 郵便番号を使用して請求データに緯度・経度情報を付与<br>
# MAGIC 5. 入力画像データに ML モデルを適用して重大度を自動推定<br>
# MAGIC 6. テレマティクスデータと請求データを結合して事故の状況（場所・速度など）を再現。道路状況や気象データなどのサードパーティデータもここに追加可能<br>
# MAGIC 7. 請求の妥当性を評価するために事前定義ルールを動的に適用し、「通常ケース」であれば支払いを迅速化<br>
# MAGIC 8. 請求調査員を支援するためダッシュボードをデータパイプラインから新たなインサイトで更新<br>
# MAGIC 
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/medallion_architecture_dlt.png" width="80%" height="80%">
# MAGIC                                                                                    
# MAGIC ETL に DLT を使用することで、オートローダーのサポート、制約によるデータ品質管理、ストリーミングワークロードの効率的な自動スケーリング、障害時の再起動による耐障害性、管理操作の実行などにより、パイプラインの簡略化と運用化が実現できる。
# MAGIC 
# MAGIC * スキーマ：smart_claims
# MAGIC * テーブル：
# MAGIC   * <b>Bronze:</b> bronze_claim, bronze_policy, bronze_accident
# MAGIC   * <b>Silver:</b> silver_claim, silver_policy, silver_claim_policy, silver_telematics, silver_accident, silver_claim_policy_accident, silver_claim_policy_telematics, silver_claim_policy_location
# MAGIC   * <b>Gold:</b> claim_rules, gold_insights
# MAGIC ___
# MAGIC 
# MAGIC # ダッシュボードによるインサイト可視化
# MAGIC <b>損失サマリー</b>ダッシュボードはビジネス全体の運営状況を俯瞰する<br>
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/summary_dashboard.png" width="60%" height="60%">
# MAGIC 
# MAGIC * <b>ロス比率（Loss Ratio）</b>は支払い保険金と調整費用の合計を獲得保険料合計で割って算出される。
# MAGIC   * 例えば、収入保険料 160 ドルに対して 80 ドルを支払った場合、ロス比率は 50% となる。
# MAGIC   * 比率が低いほど保険会社の収益性は高い。各社ごとに目標ロス比率があり、典型的な範囲は 40〜60%。
# MAGIC   * 損害は財物損害と賠償責任の 2 カテゴリーで把握され、それぞれのロス比率を個別に追跡する。
# MAGIC   * 80/20 ルールは一般的に、保険会社が保険料収入の少なくとも 80% をケアコストと品質改善活動に充てることを求める。残りの 20% は管理・間接費・マーケティングコストに充てられる。
# MAGIC * <b>サマリー</b>可視化は重大度別のインシデントタイプの件数を表示する
# MAGIC   * <b>インシデントタイプ</b>とは以下による損害を指す：
# MAGIC     * 盗難、衝突（停車中、走行中（単独・複数車両衝突））
# MAGIC   * <b>損害重大度</b>は軽微、小破、大破、全損に分類される
# MAGIC * 最近のトレンドを分析することで、近い将来の同様の請求への備えが可能になる。例えば：
# MAGIC   * 時間帯別のインシデント発生頻度・損害額の分布
# MAGIC     * ピーク時間帯などインシデントが多い時間帯はあるか？
# MAGIC   * ドライバーの年齢と標準化年齢の相関はあるか
# MAGIC     * 一定の閾値を下回る・上回るドライバーはほとんどいない点に注意
# MAGIC   * 車両の年式・製造元に対するインシデント件数の相関はどうか
# MAGIC   * 市内でインシデント発生率が高い地域（工事、渋滞、道路レイアウト、人口密度など）はどこか
# MAGIC 
# MAGIC 請求ごとの<b>調査</b>ダッシュボードは、請求担当者が請求番号を選択してさまざまな側面を詳細に確認できる追加情報を提供する<br>
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/ClaimsInvestigation.png" width="80%" height="80%">
# MAGIC 
# MAGIC * 最初のパネルは<b>カウンター</b>ウィジェットを使用して以下の累積件数の統計を表示する：
# MAGIC   * 申請された請求件数と、そのうちフラグ立てされたものの件数：
# MAGIC     * 不審なもの
# MAGIC     * ポリシーが失効していたもの
# MAGIC     * 重大度評価に不一致があったもの
# MAGIC     * 請求金額がポリシー限度額を超えていたもの
# MAGIC * 次のウィジェットは<b>テーブル</b>ビューを使用して、ML 推論とルールエンジンによりパイプラインで自動スコアリングされた最近の請求を表示する
# MAGIC   * 緑のチェックマークは自動評価が請求内容と一致することを示す
# MAGIC   * 赤いバツ印はさらに手動調査が必要な不一致を示す
# MAGIC * 特定の請求にドリルダウンすると以下が確認できる：
# MAGIC   * 損傷車両の画像
# MAGIC   * 請求・ポリシー・ドライバーの詳細
# MAGIC   * テレマティクスデータが車両の走行経路を描画
# MAGIC   * 報告データと評価データのインサイトの対比
# MAGIC ___
# MAGIC # Smart Claims における Databricks の価値提案
# MAGIC * 使用した Databricks 機能：
# MAGIC   * Delta、DLT、マルチタスクワークフロー、ML・MLFlow、DBSQL クエリ・ダッシュボード
# MAGIC * 統合 Lakehouse アーキテクチャにより：
# MAGIC   * すべてのデータペルソナが単一のプラットフォームで協力し、単一のパイプラインに貢献できる
# MAGIC   * ストリーミング・ML・BI・DE・Ops を含むすべてのビッグデータアーキテクチャパラダイムに対応
# MAGIC * ワークフローパイプラインの作成・監視・維持が容易：
# MAGIC   * マルチタスクワークフローは複数のノードタイプ（ノートブック・DLT・ML タスク・SQL ダッシュボード）に対応し、修復・再実行やコンピュート共有をサポート
# MAGIC   * DLT パイプラインは品質制約と開発ワークロードから本番への迅速な移行を提供
# MAGIC   * REST API による堅牢なスケーラブルな完全自動化でチームのアジリティと生産性を向上
# MAGIC * BI・AI ワークロード：
# MAGIC   * MLFlow で作成・管理され、再現性と監査性を確保
# MAGIC   * 作成・移植を問わず任意のモデルをサポート
# MAGIC   * Lake 内のすべてのデータにアクセス可能なパラメータ化ダッシュボードを数分で構築可能
# MAGIC ___
# MAGIC # このデモの最適な活用方法
# MAGIC * 推奨時間：1 時間（録画デモ・デッキ・フィールドデモリンク参照）
# MAGIC * 推奨対象：技術系・ビジネス系が混在する参加者（Databricks の基本知識を前提）
# MAGIC * 最良のエクスペリエンスのために：稼働中の ML ランタイムインタラクティブクラスター、DBSQL ウェアハウス、開発モードの DLT を用意してクラスター起動時間を短縮する
# MAGIC * 推奨フロー：
# MAGIC   * 「スマートクレームス」を通じた請求自動化の必要性と Lakehouse の役割を説明
# MAGIC   * デッキ：このReadmeに基づいてストーリーの流れを設定（15 分）
# MAGIC   * 現状のディスカバリー（10 分）
# MAGIC   * デモ（25 分）
# MAGIC     * データソース・EDA ノートブック
# MAGIC     * DE：ワークフロー・DLT パイプライン（5 分）
# MAGIC     * ML：モデル管理・推論（5 分）
# MAGIC     * BI：損失サマリー・請求調査（10 分）
# MAGIC  * 次のステップ（5 分）
# MAGIC ___
# MAGIC <anindita.mahapatra@databricks.com> <br>
# MAGIC <marzi.rasooli@databricks.com> <br>
# MAGIC <sara.slone@databricks.com> <br>
# MAGIC ___
# MAGIC 
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | ライブラリ                              | 説明                          | ライセンス | ソース                                              |
# MAGIC |----------------------------------------|-------------------------------|------------|-----------------------------------------------------|
# MAGIC | geopy                                  | ジオコーディング用 Python クライアント | MIT        | https://github.com/geopy/geopy                     |
# MAGIC 
# MAGIC 
# MAGIC ## はじめに
# MAGIC 
# MAGIC 特定のソリューションは .dbc アーカイブとして当社ウェブサイトからダウンロードすることもできますが、Databricks 環境にこれらのリポジトリをクローンすることを推奨します。最新のコードにアクセスできるだけでなく、業界のベストプラクティスを推進し再利用可能なソリューションを生み出すエキスパートコミュニティの一員となることができます。
# MAGIC 
# MAGIC <img width="500" alt="add_repo" src="https://user-images.githubusercontent.com/4445837/177207338-65135b10-8ccc-4d17-be21-09416c861a76.png">
# MAGIC 
# MAGIC Databricks でソリューションアクセラレーターの使用を開始するには、以下の手順に従ってください：
# MAGIC 
# MAGIC 1. [Databricks Repos](https://www.databricks.com/product/repos) を使用して、Databricks 内にソリューションアクセラレーターのリポジトリをクローンする
# MAGIC 2. `RUNME` ノートブックを任意のクラスターに接続し、Run-All でノートブックを実行する。アクセラレーターのパイプラインを記述したマルチステップジョブが作成され、リンクが表示される。ジョブ設定は RUNME ノートブック内に JSON 形式で記述されている。
# MAGIC 3. マルチステップジョブを実行してパイプラインの動作を確認する。
# MAGIC 4. ソリューションアクセラレーターのサンプルを自分のニーズに合わせて変更したり、他のユーザーと協力して独自のデータに対してコードサンプルを実行したりすることができる。そのためには、まずリポジトリの Git リモートを当社のサンプルリポジトリから自組織のリポジトリに変更する（詳細を参照）。その後、コードのコミット・プッシュ、他ユーザーとの Git を通じた協業、組織のコード開発プロセスに従った作業が可能になる。
# MAGIC 
# MAGIC アクセラレーターの実行に伴うコストはユーザーの責任となります。
# MAGIC 
# MAGIC 
# MAGIC ## プロジェクトのサポートについて
# MAGIC 
# MAGIC このプロジェクトのコードは探索目的のみに提供されており、Databricks による SLA（サービスレベルアグリーメント）のある正式なサポートは提供されていません。AS-IS（現状のまま）で提供されており、いかなる保証も行いません。これらのプロジェクトの使用に起因する問題についてサポートチケットを提出しないようにしてください。このプロジェクトのソースコードは Databricks [ライセンス](./LICENSE) に準拠して提供されます。含まれるまたは参照されるすべてのサードパーティライブラリは、以下に記載のライセンスに従います。
# MAGIC 
# MAGIC このプロジェクトの使用を通じて発見された問題は、リポジトリの GitHub Issues として提出してください。時間の許す限りレビューされますが、サポートに関する正式な SLA はありません。

# COMMAND ----------


