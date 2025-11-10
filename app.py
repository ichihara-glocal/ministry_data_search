import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ----------------------------------------------------------------------
# ページ設定
# ----------------------------------------------------------------------
st.set_page_config(
    page_title="省庁資料検索ツール (Streamlit版)",
    layout="wide"
)

# ----------------------------------------------------------------------
# カラム名の設定（日本語表示名）
# ----------------------------------------------------------------------
COLUMN_NAMES = {
    'file_id': 'ファイルID',
    'title': '資料名',
    'ministry': '省庁',
    'fiscal_year_start': '年度',
    'category': 'カテゴリ',
    'sub_category': '資料形式',
    'file_page': 'ページ',
    'source_url': 'URL',
    'content_text': '本文'
}

# ----------------------------------------------------------------------
# BigQuery 接続
# ----------------------------------------------------------------------

@st.cache_resource
def get_bigquery_client():
    """
    StreamlitのsecretsからGCPサービスアカウントキーを取得し、
    BigQueryクライアントを初期化します。
    """
    try:
        creds_json = st.secrets["gcp_service_account"] 
        project_id = st.secrets['bigquery']['project_id']
        
        creds = service_account.Credentials.from_service_account_info(creds_json)
        client = bigquery.Client(credentials=creds, project=project_id)
        client.list_projects(max_results=1)
        
        return client
    except Exception as e:
        st.error(f"BigQuery接続エラー: {e}")
        st.stop()

# ----------------------------------------------------------------------
# 認証とセッション管理
# ----------------------------------------------------------------------

if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
if 'user_id' not in st.session_state:
    st.session_state['user_id'] = ""

def log_login_to_bigquery(_bq_client, input_user_id, input_password, login_result, current_session_id):
    """
    ログイン試行ログをBigQueryに保存します。
    """
    log_table_id = (
        f"{st.secrets['bigquery']['project_id']}"
        f".{st.secrets['bigquery']['config_dataset']}"
        f".{st.secrets['bigquery']['log_login_table']}"
    )
    
    try:
        rows_to_insert = [
            {
                "timestamp": pd.Timestamp.now(tz='Asia/Tokyo').isoformat(),
                "id": input_user_id,
                "password": input_password,
                "result": login_result,
                "sessionId": current_session_id 
            }
        ]
        
        _bq_client.insert_rows_json(log_table_id, rows_to_insert)
    except Exception as e:
        st.warning(f"ログ記録エラー: {e}")

def check_credentials_bigquery(bq_client, user_id, password):
    """
    BigQueryの認証テーブルをチェックします。
    """
    auth_table_id_str = (
        f"`{st.secrets['bigquery']['project_id']}"
        f".{st.secrets['bigquery']['config_dataset']}"
        f".{st.secrets['bigquery']['auth_table']}`"
    )
    
    try:
        query = f"""
            SELECT id 
            FROM {auth_table_id_str}
            WHERE id = @user_id AND pw = @password
            LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
                bigquery.ScalarQueryParameter("password", "STRING", password),
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.to_dataframe()
        
        return not results.empty
        
    except Exception as e:
        st.error(f"認証エラー: {e}")
        return False

def show_login_form(bq_client):
    """
    ログインフォームを表示します。
    """
    st.title("省庁資料検索ツール（PoC版） - ログイン")
    
    with st.form("login_form"):
        user_id = st.text_input("ユーザーID")
        password = st.text_input("パスワード", type="password")
        submitted = st.form_submit_button("ログイン")

        if submitted:
            if not user_id or not password:
                st.error("ユーザーIDとパスワードを入力してください。")
                return

            with st.spinner("認証中..."):
                if check_credentials_bigquery(bq_client, user_id, password):
                    st.session_state['authenticated'] = True
                    st.session_state['user_id'] = user_id
                    log_login_to_bigquery(bq_client, user_id, password, 'success', user_id)
                    st.rerun()
                else:
                    log_login_to_bigquery(bq_client, user_id, password, 'failed', user_id)
                    st.error("ユーザーIDまたはパスワードが間違っています。")

# ----------------------------------------------------------------------
# メインアプリケーション
# ----------------------------------------------------------------------

@st.cache_data(ttl=3600)
def load_metadata(_bq_client):
    """
    フィルタ用のメタデータをBigQueryから読み込みます。
    """
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
        st.error(f"メタデータの読み込みエラー: {e}")
        return pd.DataFrame()

def run_search(_bq_client, keyword, ministries, categories, sub_categories, years):
    """
    検索クエリを実行します。
    """
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
        
    final_query += " ORDER BY ministry, category, fiscal_year_start"

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    
    try:
        df = _bq_client.query(final_query, job_config=job_config).to_dataframe()
        # カラム名を日本語に変換
        df = df.rename(columns=COLUMN_NAMES)
        return df
    except Exception as e:
        st.error(f"検索エラー: {e}")
        return pd.DataFrame()

def log_search_to_bigquery(_bq_client, keyword, ministries, categories, sub_categories, years, file_count, page_count):
    """
    検索ログをBigQueryに保存します。
    """
    log_table_id = (
        f"{st.secrets['bigquery']['project_id']}"
        f".{st.secrets['bigquery']['config_dataset']}"
        f".{st.secrets['bigquery']['log_search_table']}"
    )
    
    try:
        rows_to_insert = [
            {
                "timestamp": pd.Timestamp.now(tz='Asia/Tokyo').isoformat(),
                "sessionId": st.session_state['user_id'],
                "keyword": keyword,
                "filter_ministries": ", ".join(ministries), 
                "filter_category": ", ".join(categories),
                "filter_subcategory": ", ".join(sub_categories),
                "filter_year": ", ".join([str(y) for y in years]),
                "file_count": file_count,
                "page_count": page_count
            }
        ]
        
        _bq_client.insert_rows_json(log_table_id, rows_to_insert)
    except Exception as e:
        st.warning(f"検索ログ記録エラー: {e}")

def main_app(bq_client):
    """
    認証後に表示されるメインアプリケーション
    """
    st.title("省庁資料検索ツール（Streamlit版）")
    
    # サイドバー (フィルタ)
    st.sidebar.header("🔽 条件絞り込み")
    
    keyword = st.sidebar.text_input("キーワード", placeholder="キーワードを入力")
    
    st.sidebar.markdown("---")
    
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
    
    # 検索ボタン（赤色）
    search_button = st.sidebar.button("🔍 検索", type="primary", use_container_width=True)
    
    st.sidebar.markdown("")
    
    if st.sidebar.button("フィルタをリセット", use_container_width=True):
        st.rerun()
    
    st.sidebar.markdown("")
    
    if st.sidebar.button("ログアウト", use_container_width=True):
        st.session_state['authenticated'] = False
        st.session_state['user_id'] = ""
        st.rerun()

    # メインコンテンツ (検索結果)
    st.markdown("---")

    if search_button:
        with st.spinner("🔄 検索中..."):
            results_df = run_search(bq_client, keyword, ministries, categories, sub_categories, years)
            
            if not results_df.empty:
                page_count = len(results_df)
                # 日本語カラム名に変更後は 'ファイルID' を使用
                file_count = results_df[COLUMN_NAMES['file_id']].nunique()
                
                st.success(f"{file_count}ファイル・{page_count}ページ ヒットしました")
                
                log_search_to_bigquery(
                    bq_client, keyword, ministries, categories, 
                    sub_categories, [str(y) for y in years], file_count, page_count
                )
                
                # データフレームを縦長表示（高さ2000px）
                # column_configでURLをハイパーリンク化
                st.dataframe(
                    results_df, 
                    height=2000, 
                    use_container_width=True,
                    column_config={
                        COLUMN_NAMES['source_url']: st.column_config.LinkColumn(
                            COLUMN_NAMES['source_url'],
                            display_text="📄リンク"
                        )
                    }
                )
                
            else:
                st.info("該当する結果が見つかりませんでした。")

# ----------------------------------------------------------------------
# アプリケーションの実行
# ----------------------------------------------------------------------

bq_client = get_bigquery_client()

if not st.session_state['authenticated']:
    show_login_form(bq_client)
else:
    main_app(bq_client)