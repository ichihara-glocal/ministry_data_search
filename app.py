import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json

# ----------------------------------------------------------------------
# ページ設定
# ----------------------------------------------------------------------
st.set_page_config(
    page_title="省庁資料検索ツール (Streamlit版)",
    layout="wide"
)

# ----------------------------------------------------------------------
# BigQuery 接続
# (認証とデータ取得の両方で使用)
# ----------------------------------------------------------------------

@st.cache_resource # クライアントはリソースとしてキャッシュ
def get_bigquery_client():
    """
    StreamlitのsecretsからGCPサービスアカウントキーを取得し、
    BigQueryクライアントを初期化します。
    """
    try:
        # st.secretsがTOMLテーブルとして直接辞書を返すため、json.loads()は不要
        creds_json = st.secrets["gcp_service_account"] 
        
        creds = service_account.Credentials.from_service_account_info(creds_json)
        # BigQueryクライアントの初期化時にプロジェクトIDを明示的に指定
        client = bigquery.Client(credentials=creds, project=st.secrets['bigquery']['project_id'])
        return client
    except Exception as e:
        # エラーメッセージを分かりやすく
        st.error(f"🚨 BigQueryクライアントの初期化に失敗しました。secrets.tomlの設定を確認してください: {e}")
        st.stop()

# ----------------------------------------------------------------------
# 認証とセッション管理
# ----------------------------------------------------------------------

# セッションステートの初期化
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
if 'user_id' not in st.session_state:
    st.session_state['user_id'] = ""

def log_login_to_bigquery(_bq_client, user_id, status):
    """
    ログイン試行ログをBigQueryのconfigデータセットに保存します。
    """
    try:
        log_table_id = (
            f"{st.secrets['bigquery']['project_id']}"
            f".{st.secrets['bigquery']['config_dataset']}"
            f".{st.secrets['bigquery']['log_login_table']}"
        )
        
        rows_to_insert = [
            {
                "timestamp": pd.Timestamp.now(tz='Asia/Tokyo').isoformat(),
                "session_id": user_id, # ここでは、試行したユーザーIDをセッション識別子の代わりとして記録
                "status": status # 'success' or 'failed'
            }
        ]
        
        errors = _bq_client.insert_rows_json(log_table_id, rows_to_insert)
        if errors == []:
            print(f"ログインログ ({status}) をBigQueryに保存しました。")
        else:
            # BigQueryエラーを詳細に出力
            print(f"BigQueryへのログインログ保存に失敗しました: {errors}")
            
    except Exception as e:
        # ログ失敗はアプリの停止を妨げないが警告
        st.warning(f"ログ記録機能でエラーが発生しました: {e}")

def check_credentials_bigquery(bq_client, user_id, password):
    """
    BigQueryの認証テーブルをチェックします。
    """
    try:
        auth_table_id = (
            f"`{st.secrets['bigquery']['project_id']}"
            f".{st.secrets['bigquery']['config_dataset']}" # 認証用データセット
            f".{st.secrets['bigquery']['auth_table']}`"
        )
        
        # SQLインジェクション対策としてパラメータ化クエリを使用
        # configデータセットへのSELECT権限が必要です
        query = f"""
            SELECT id 
            FROM {auth_table_id}
            WHERE id = @user_id AND pw = @password
            LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
                bigquery.ScalarQueryParameter("password", "STRING", password),
            ]
        )
        
        # クエリ実行
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.to_dataframe() # 結果を取得
        
        # 該当するユーザーがいれば認証成功
        return not results.empty
        
    except Exception as e:
        # 認証クエリ実行エラーは、認証失敗として扱う
        print(f"認証クエリ実行エラー: {e}")
        st.error("認証テーブルへのアクセス中にエラーが発生しました。権限とテーブル名を確認してください。")
        return False

def show_login_form(bq_client):
    """
    ログインフォームを表示します。
    """
    st.title("省庁資料検索ツール（PoC版） - ログイン")
    # ログインIDとPWのテーブル構成を表示 (デバッグ用)
    st.caption(f"認証テーブル: `{st.secrets['bigquery']['project_id']}.{st.secrets['bigquery']['config_dataset']}.{st.secrets['bigquery']['auth_table']}`")
    
    with st.form("login_form"):
        user_id = st.text_input("ユーザーID")
        password = st.text_input("パスワード", type="password")
        submitted = st.form_submit_button("ログイン")

        if submitted:
            if not user_id or not password:
                st.error("ユーザーIDとパスワードを入力してください。")
                return

            with st.spinner("認証中..."):
                try:
                    # BigQueryで認証実行
                    if check_credentials_bigquery(bq_client, user_id, password):
                        st.session_state['authenticated'] = True
                        st.session_state['user_id'] = user_id
                        
                        # ログイン成功ログをBigQueryに記録
                        log_login_to_bigquery(bq_client, user_id, 'success')
                        
                        st.rerun() # 認証成功したらページを再読み込み
                    else:
                        # ログイン失敗ログをBigQueryに記録
                        log_login_to_bigquery(bq_client, user_id, 'failed')
                        st.error("ユーザーIDまたはパスワードが間違っています。")
                except Exception as e:
                    # 予期せぬエラーが発生した場合
                    st.error(f"ログイン処理中に予期せぬエラーが発生しました: {e}")

# ----------------------------------------------------------------------
# メインアプリケーション
# ----------------------------------------------------------------------

@st.cache_data(ttl=3600) # 1時間キャッシュ
def load_metadata(_bq_client):
    """
    フィルタ用のメタデータをBigQueryから読み込みます。
    """
    # データ検索用のデータセットを参照
    query = f"""
      SELECT 
        ministry,
        category,
        sub_category,
        fiscal_year_start
      FROM `{st.secrets["bigquery"]["project_id"]}.{st.secrets["bigquery"]["dataset"]}.{st.secrets["bigquery"]["table"]}`
      GROUP BY ministry, category, sub_category, fiscal_year_start
      ORDER BY ministry, category, sub_category, fiscal_year_start
    """
    try:
        df = _bq_client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"メタデータの読み込みに失敗しました。データテーブルの権限とテーブル名を確認してください: {e}")
        return pd.DataFrame()

def run_search(_bq_client, keyword, ministries, categories, sub_categories, years):
    """
    検索クエリを実行します。
    """
    # データ検索用のデータセットを参照
    base_query = f"""
        SELECT 
            file_id, title, ministry, fiscal_year_start, category, 
            sub_category, file_page, source_url, content_text
        FROM `{st.secrets["bigquery"]["project_id"]}.{st.secrets["bigquery"]["dataset"]}.{st.secrets["bigquery"]["table"]}`
    """
    
    where_conditions = []
    query_params = [] # SQLインジェクション対策

    # PythonならWHERE句の構築が簡単です
    if ministries:
        where_conditions.append("ministry IN UNNEST(@ministries)")
        query_params.append(bigquery.ArrayQueryParameter("ministries", "STRING", ministries))
        
    if categories:
        where_conditions.append("category IN UNNEST(@categories)")
        query_params.append(bigquery.ArrayQueryParameter("categories", "STRING", categories))

    if sub_categories:
        where_conditions.append("sub_category IN UNNEST(@sub_categories)")
        query_params.append(bigquery.ArrayQueryParameter("sub_categories", "STRING", sub_categories))

    if years:
        # 年度はINT64として扱う
        int_years = [int(y) for y in years]
        where_conditions.append("fiscal_year_start IN UNNEST(@years)")
        query_params.append(bigquery.ArrayQueryParameter("years", "INT64", int_years))

    if keyword:
        where_conditions.append("(LOWER(title) LIKE @keyword OR LOWER(content_text) LIKE @keyword)")
        query_params.append(bigquery.ScalarQueryParameter("keyword", "STRING", f"%{keyword.lower()}%"))

    # クエリを結合
    if where_conditions:
        final_query = base_query + " WHERE " + " AND ".join(where_conditions)
    else:
        final_query = base_query
        
    final_query += " ORDER BY ministry, category, fiscal_year_start LIMIT 1000" # 念のためリミット

    # BigQueryジョブの設定
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    
    try:
        df = _bq_client.query(final_query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        st.error(f"検索エラー: {e}")
        return pd.DataFrame()

def log_search_to_bigquery(_bq_client, keyword, ministries, categories, sub_categories, years, file_count, page_count):
    """
    検索ログをBigQueryの別テーブルに保存します。
    """
    try:
        # ログ用のデータセットとテーブル情報をsecretsから取得
        log_table_id = (
            f"{st.secrets['bigquery']['project_id']}"
            f".{st.secrets['bigquery']['config_dataset']}" # ログ・設定用データセット
            f".{st.secrets['bigquery']['log_search_table']}" # secrets.tomlで指定
        )
        
        rows_to_insert = [
            {
                "timestamp": pd.Timestamp.now(tz='Asia/Tokyo').isoformat(),
                "session_id": st.session_state['user_id'], # ユーザーIDをセッションID代わりに使用
                "keyword": keyword,
                "ministries": ", ".join(ministries),
                "categories": ", ".join(categories),
                "sub_categories": ", ".join(sub_categories),
                "years": ", ".join(years),
                "file_count": file_count,
                "page_count": page_count
            }
        ]
        
        errors = _bq_client.insert_rows_json(log_table_id, rows_to_insert)
        if errors == []:
            print("検索ログをBigQueryに保存しました。")
        else:
            print(f"BigQueryへのログ保存に失敗しました: {errors}")
            
    except Exception as e:
        st.warning(f"検索ログの保存に失敗しました: {e} (ログテーブル: {log_table_id})")


def main_app(bq_client):
    """
    認証後に表示されるメインアプリケーション
    """
    st.title("省庁資料検索ツール（Streamlit版）")
    
    # -----------------
    # 1. サイドバー (フィルタ)
    # -----------------
    st.sidebar.header("🔽 条件絞り込み")
    
    with st.spinner("フィルタを読み込み中..."):
        meta_df = load_metadata(bq_client)
    
    if meta_df.empty:
        st.sidebar.error("フィルタの読み込みに失敗しました。")
        st.stop()

    # GASの 'renderCheckboxes' を st.multiselect で再現
    ministries = st.sidebar.multiselect(
        "省庁:",
        sorted(meta_df['ministry'].unique())
    )
    categories = st.sidebar.multiselect(
        "カテゴリ:",
        sorted(meta_df['category'].unique())
    )
    sub_categories = st.sidebar.multiselect(
        "資料形式:",
        sorted(meta_df['sub_category'].unique())
    )
    years = st.sidebar.multiselect(
        "年度:",
        sorted(meta_df['fiscal_year_start'].unique(), reverse=True)
    )

    st.sidebar.markdown("---")
    if st.sidebar.button("フィルタをリセット"):
        # Streamlitではウィジェットをクリアするより、ページをリランするのが簡単
        st.rerun()

    # -----------------
    # 2. メインコンテンツ (検索と結果)
    # -----------------
    
    # キーワード入力
    keyword = st.text_input("キーワード", placeholder="キーワードを入力")
    
    # 検索ボタン
    search_button = st.button("検索")
    
    st.markdown("---")

    if search_button:
        with st.spinner("🔄 検索中..."):
            # 検索実行
            results_df = run_search(bq_client, keyword, ministries, categories, sub_categories, years)
            
            if not results_df.empty:
                page_count = len(results_df)
                file_count = results_df['file_id'].nunique()
                
                st.success(f"{file_count}ファイル・{page_count}ページ ヒットしました")
                
                # 検索ログをBigQueryに記録 (ステップ5)
                log_search_to_bigquery(
                    bq_client, keyword, ministries, categories, 
                    sub_categories, [str(y) for y in years], file_count, page_count
                )
                
                # 結果をデータフレームとして表示 (ソートやフィルタリングが標準装備)
                st.dataframe(results_df)
                
            else:
                st.info("該当する結果が見つかりませんでした。")

# ----------------------------------------------------------------------
# アプリケーションの実行
# ----------------------------------------------------------------------

# まずBQクライアントを初期化
bq_client = get_bigquery_client()

# セッションステートをチェックして、認証済みか判断
if not st.session_state['authenticated']:
    show_login_form(bq_client)
else:
    main_app(bq_client)