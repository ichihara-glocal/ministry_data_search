import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import traceback

# ----------------------------------------------------------------------
# ページ設定
# ----------------------------------------------------------------------
st.set_page_config(
    page_title="省庁資料検索ツール (Streamlit版)",
    layout="wide"
)

# ----------------------------------------------------------------------
# デバッグモード設定
# ----------------------------------------------------------------------
DEBUG_MODE = True  # デバッグ情報を表示する場合はTrue

def debug_log(message):
    """デバッグログをターミナルとStreamlitに出力"""
    if DEBUG_MODE:
        print(f"[DEBUG] {message}")
        # st.caption(f"🐛 {message}")  # 画面にも表示したい場合はコメント解除

# ----------------------------------------------------------------------
# BigQuery 接続
# ----------------------------------------------------------------------

@st.cache_resource
def get_bigquery_client():
    """
    StreamlitのsecretsからGCPサービスアカウントキーを取得し、
    BigQueryクライアントを初期化します。
    """
    debug_log("BigQueryクライアント初期化開始")
    try:
        creds_json = st.secrets["gcp_service_account"] 
        project_id = st.secrets['bigquery']['project_id']
        
        debug_log(f"プロジェクトID: {project_id}")
        
        creds = service_account.Credentials.from_service_account_info(creds_json)
        client = bigquery.Client(credentials=creds, project=project_id)

        # 接続テスト
        debug_log("BigQuery接続テスト中...")
        list(client.list_projects(max_results=1))
        debug_log("BigQuery接続成功")
        
        return client
    except Exception as e:
        error_msg = f"BigQuery初期接続エラー: {e}"
        debug_log(error_msg)
        debug_log(traceback.format_exc())
        st.error(f"🚨 {error_msg}")
        st.caption("詳細: サービスアカウントのJSONキー、`secrets.toml` の `project_id`、および `BigQuery ジョブユーザー` 権限を確認してください。")
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
    debug_log(f"ログイン記録開始: user_id={user_id}, status={status}")
    try:
        log_table_id = (
            f"{st.secrets['bigquery']['project_id']}"
            f".{st.secrets['bigquery']['config_dataset']}"
            f".{st.secrets['bigquery']['log_login_table']}"
        )
        
        debug_log(f"ログテーブル: {log_table_id}")
        
        rows_to_insert = [
            {
                "timestamp": pd.Timestamp.now(tz='Asia/Tokyo').isoformat(),
                "session_id": user_id,
                "status": status
            }
        ]
        
        errors = _bq_client.insert_rows_json(log_table_id, rows_to_insert)
        if errors == []:
            debug_log(f"ログインログ ({status}) をBigQueryに保存成功")
        else:
            debug_log(f"BigQueryログ保存エラー: {errors}")
            
    except Exception as e:
        error_msg = f"ログ記録エラー: {e}"
        debug_log(error_msg)
        debug_log(traceback.format_exc())
        st.warning(error_msg)

def check_credentials_bigquery(bq_client, user_id, password):
    """
    BigQueryの認証テーブルをチェックします。
    """
    debug_log(f"認証チェック開始: user_id={user_id}")
    
    auth_table_id_str = (
        f"`{st.secrets['bigquery']['project_id']}"
        f".{st.secrets['bigquery']['config_dataset']}"
        f".{st.secrets['bigquery']['auth_table']}`"
    )
    
    debug_log(f"認証テーブル: {auth_table_id_str}")
    
    try:
        query = f"""
            SELECT id 
            FROM {auth_table_id_str}
            WHERE id = @user_id AND pw = @password
            LIMIT 1
        """
        
        debug_log("クエリ準備完了")
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
                bigquery.ScalarQueryParameter("password", "STRING", password),
            ]
        )
        
        debug_log("クエリ実行開始...")
        query_job = bq_client.query(query, job_config=job_config)
        
        debug_log("結果取得中...")
        results = query_job.to_dataframe()
        
        debug_log(f"クエリ実行完了: 結果行数={len(results)}")
        
        # 該当するユーザーがいれば認証成功
        is_authenticated = not results.empty
        debug_log(f"認証結果: {is_authenticated}")
        
        return is_authenticated
        
    except Exception as e:
        error_msg = f"認証クエリエラー: {e}"
        debug_log(error_msg)
        debug_log(traceback.format_exc())
        
        # エラー情報をセッションステートに保存
        st.session_state['auth_error'] = str(e)
        st.session_state['auth_table'] = auth_table_id_str
        return False

def show_login_form(bq_client):
    """
    ログインフォームを表示します。
    """
    st.title("省庁資料検索ツール（PoC版） - ログイン")
    
    # デバッグ情報表示
    if DEBUG_MODE:
        with st.expander("🐛 デバッグ情報"):
            st.write("セッションステート:", st.session_state)
    
    with st.form("login_form"):
        user_id = st.text_input("ユーザーID")
        password = st.text_input("パスワード", type="password")
        submitted = st.form_submit_button("ログイン")

    # フォームの外で処理
    if submitted:
        debug_log("ログインボタン押下")
        
        # セッションステートのエラーをクリア
        if 'auth_error' in st.session_state:
            del st.session_state['auth_error']
        if 'auth_table' in st.session_state:
            del st.session_state['auth_table']

        if not user_id or not password:
            debug_log("ユーザーIDまたはパスワードが空")
            st.error("ユーザーIDとパスワードを入力してください。")
            st.stop()

        # スピナーで待機状態を示す
        with st.spinner("認証中..."):
            debug_log("認証処理開始")
            auth_result = check_credentials_bigquery(bq_client, user_id, password)
            debug_log(f"認証処理完了: result={auth_result}")
        
        # スピナーの外でエラーチェック
        if 'auth_error' in st.session_state:
            debug_log("認証エラー検出")
            st.error(f"認証クエリ実行エラーが発生しました: {st.session_state['auth_error']}")
            st.caption(f"認証を試みたテーブル: {st.session_state['auth_table']}")
            log_login_to_bigquery(bq_client, user_id, 'failed')
            st.stop()
        
        if auth_result:
            debug_log("認証成功")
            st.session_state['authenticated'] = True
            st.session_state['user_id'] = user_id
            
            # ログイン成功ログをBigQueryに記録
            log_login_to_bigquery(bq_client, user_id, 'success')
            
            st.success("ログインに成功しました！")
            debug_log("ページ再読み込み実行")
            st.rerun()
        else:
            debug_log("認証失敗")
            # 認証失敗ログをBigQueryに記録
            log_login_to_bigquery(bq_client, user_id, 'failed')
            st.error("ユーザーIDまたはパスワードが間違っています。")

# ----------------------------------------------------------------------
# メインアプリケーション
# ----------------------------------------------------------------------

@st.cache_data(ttl=3600)
def load_metadata(_bq_client):
    """
    フィルタ用のメタデータをBigQueryから読み込みます。
    """
    debug_log("メタデータ読み込み開始")
    
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
        debug_log(f"メタデータ読み込み成功: {len(df)}行")
        return df
    except Exception as e:
        error_msg = f"メタデータ読み込みエラー: {e}"
        debug_log(error_msg)
        debug_log(traceback.format_exc())
        st.error(error_msg)
        return pd.DataFrame()

def run_search(_bq_client, keyword, ministries, categories, sub_categories, years):
    """
    検索クエリを実行します。
    """
    debug_log(f"検索開始: keyword={keyword}, ministries={ministries}")
    
    base_query = f"""
        SELECT 
            file_id, title, ministry, fiscal_year_start, category, 
            sub_category, file_page, source_url, content_text
        FROM `{st.secrets["bigquery"]["project_id"]}.{st.secrets["bigquery"]["dataset"]}.{st.secrets["bigquery"]["table"]}`
    """
    
    where_conditions = []
    query_params = []

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
        int_years = [int(y) for y in years]
        where_conditions.append("fiscal_year_start IN UNNEST(@years)")
        query_params.append(bigquery.ArrayQueryParameter("years", "INT64", int_years))

    if keyword:
        where_conditions.append("(LOWER(title) LIKE @keyword OR LOWER(content_text) LIKE @keyword)")
        query_params.append(bigquery.ScalarQueryParameter("keyword", "STRING", f"%{keyword.lower()}%"))

    if where_conditions:
        final_query = base_query + " WHERE " + " AND ".join(where_conditions)
    else:
        final_query = base_query
        
    final_query += " ORDER BY ministry, category, fiscal_year_start LIMIT 1000"

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    
    try:
        debug_log("検索クエリ実行中...")
        df = _bq_client.query(final_query, job_config=job_config).to_dataframe()
        debug_log(f"検索完了: {len(df)}件")
        return df
    except Exception as e:
        error_msg = f"検索エラー: {e}"
        debug_log(error_msg)
        debug_log(traceback.format_exc())
        st.error(error_msg)
        return pd.DataFrame()

def log_search_to_bigquery(_bq_client, keyword, ministries, categories, sub_categories, years, file_count, page_count):
    """
    検索ログをBigQueryの別テーブルに保存します。
    """
    debug_log(f"検索ログ記録開始: keyword={keyword}, results={file_count}files/{page_count}pages")
    
    try:
        log_table_id = (
            f"{st.secrets['bigquery']['project_id']}"
            f".{st.secrets['bigquery']['config_dataset']}"
            f".{st.secrets['bigquery']['log_search_table']}"
        )
        
        rows_to_insert = [
            {
                "timestamp": pd.Timestamp.now(tz='Asia/Tokyo').isoformat(),
                "session_id": st.session_state['user_id'],
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
            debug_log("検索ログ保存成功")
        else:
            debug_log(f"検索ログ保存エラー: {errors}")
            
    except Exception as e:
        error_msg = f"検索ログ保存エラー: {e}"
        debug_log(error_msg)
        st.warning(f"{error_msg} (ログテーブル: {log_table_id})")

def main_app(bq_client):
    """
    認証後に表示されるメインアプリケーション
    """
    debug_log(f"メインアプリ表示: user={st.session_state['user_id']}")
    
    st.title("省庁資料検索ツール（Streamlit版）")
    
    # ログアウトボタン
    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("ログアウト"):
            debug_log("ログアウト実行")
            st.session_state['authenticated'] = False
            st.session_state['user_id'] = ""
            st.rerun()
    
    with col1:
        st.write(f"ログイン中: **{st.session_state['user_id']}**")
    
    # デバッグ情報表示
    if DEBUG_MODE:
        with st.expander("🐛 デバッグ情報"):
            st.write("セッションステート:", st.session_state)
    
    # -----------------
    # 1. サイドバー (フィルタ)
    # -----------------
    st.sidebar.header("🔽 条件絞り込み")
    
    with st.spinner("フィルタを読み込み中..."):
        meta_df = load_metadata(bq_client)
    
    if meta_df.empty:
        st.sidebar.error("フィルタの読み込みに失敗しました。")
        st.stop()

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
        debug_log("フィルタリセット")
        st.rerun()

    # -----------------
    # 2. メインコンテンツ (検索と結果)
    # -----------------
    
    keyword = st.text_input("キーワード", placeholder="キーワードを入力")
    
    search_button = st.button("検索")
    
    st.markdown("---")

    if search_button:
        with st.spinner("🔄 検索中..."):
            results_df = run_search(bq_client, keyword, ministries, categories, sub_categories, years)
            
        if not results_df.empty:
            page_count = len(results_df)
            file_count = results_df['file_id'].nunique()
            
            st.success(f"{file_count}ファイル・{page_count}ページ ヒットしました")
            
            log_search_to_bigquery(
                bq_client, keyword, ministries, categories, 
                sub_categories, [str(y) for y in years], file_count, page_count
            )
            
            st.dataframe(results_df)
            
        else:
            st.info("該当する結果が見つかりませんでした。")

# ----------------------------------------------------------------------
# アプリケーションの実行
# ----------------------------------------------------------------------

debug_log("=" * 50)
debug_log("アプリケーション起動")
debug_log("=" * 50)

# BigQueryクライアント初期化
bq_client = get_bigquery_client()

# 認証チェック
if not st.session_state['authenticated']:
    debug_log("未認証 - ログインフォーム表示")
    show_login_form(bq_client)
else:
    debug_log("認証済み - メインアプリ表示")
    main_app(bq_client)