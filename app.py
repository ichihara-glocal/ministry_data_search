import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import gspread  # Googleスプレッドシート操作用
import json

# ----------------------------------------------------------------------
# ページ設定
# ----------------------------------------------------------------------
st.set_page_config(
    page_title="省庁資料検索ツール (Streamlit版)",
    layout="wide"
)

# ----------------------------------------------------------------------
# 認証とセッション管理
# ----------------------------------------------------------------------

# セッションステートの初期化
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
if 'user_id' not in st.session_state:
    st.session_state['user_id'] = ""

def get_gspread_client():
    """
    StreamlitのsecretsからGoogle Service Accountキーを取得し、
    gspreadクライアントを認証・初期化します。
    """
    # st.secretsからサービスアカウント情報を取得
    creds_json = st.secrets["gcp_service_account"]
    
    # gspreadが要求するスコープ
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    # 認証情報を作成
    creds = service_account.Credentials.from_service_account_info(
        creds_json,
        scopes=scopes
    )
    
    # gspreadクライアントを認証
    client = gspread.authorize(creds)
    return client

def check_credentials(client, user_id, password):
    """
    Googleスプレッドシート（認証DB）をチェックします。
    GASの'validateCredentials'関数のStreamlit版です。
    """
    try:
        # secretsからスプレッドシートIDを取得
        auth_sheet_id = st.secrets["google_sheets"]["auth_spreadsheet_id"]
        
        # スプレッドシートを開く
        sheet = client.open_by_key(auth_sheet_id).worksheet("auth") # シート名を'auth'と仮定
        
        # 全データを取得（pandas DataFrameとして読み込むと便利）
        data = pd.DataFrame(sheet.get_all_records())
        
        if data.empty:
            st.error("認証シートが空です。")
            return False

        # 認証ロジック (GASのロジックを再現)
        # 'user'と'password'はスプレッドシートのカラム名と仮定
        user_row = data[(data['id'] == user_id) & (data['pw'] == password)]
        
        return not user_row.empty
        
    except Exception as e:
        st.error(f"認証シートへのアクセスエラー: {e}")
        return False

def show_login_form():
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
                try:
                    # gspreadクライアントを取得
                    gs_client = get_gspread_client()
                    
                    # 認証実行
                    if check_credentials(gs_client, user_id, password):
                        st.session_state['authenticated'] = True
                        st.session_state['user_id'] = user_id
                        
                        # TODO: ここでログインログをBigQueryに記録 (ステップ5)
                        
                        st.rerun() # 認証成功したらページを再読み込み
                    else:
                        st.error("ユーザーIDまたはパスワードが間違っています。")
                except Exception as e:
                    st.error(f"ログイン処理中にエラーが発生しました: {e}")

# ----------------------------------------------------------------------
# BigQuery 接続
# ----------------------------------------------------------------------

def get_bigquery_client():
    """
    StreamlitのsecretsからGCPサービスアカウントキーを取得し、
    BigQueryクライアントを初期化します。
    """
    creds_json = st.secrets["gcp_service_account"]
    creds = service_account.Credentials.from_service_account_info(creds_json)
    client = bigquery.Client(credentials=creds, project=creds.project_id)
    return client

# ----------------------------------------------------------------------
# メインアプリケーション
# ----------------------------------------------------------------------

@st.cache_data(ttl=3600) # 1時間キャッシュ
def load_metadata(_bq_client):
    """
    フィルタ用のメタデータをBigQueryから読み込みます。
    GASの 'getMetadataSummary' のStreamlit版です。
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
        st.error(f"メタデータの読み込みに失敗しました: {e}")
        return pd.DataFrame()

def run_search(_bq_client, keyword, ministries, categories, sub_categories, years):
    """
    検索クエリを実行します。
    GASの 'getSearchResults' と 'buildWhereClause' のStreamlit版です。
    """
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
    GASの 'logSearchToSheet' のStreamlit版（BigQuery移行版）です。
    """
    try:
        log_table_id = f"{st.secrets['bigquery']['project_id']}.{st.secrets['bigquery']['dataset']}.log_search" # 仮のテーブル名
        
        rows_to_insert = [
            {
                "timestamp": pd.Timestamp.now(tz='Asia/Tokyo').isoformat(),
                "session_id": st.session_state['user_id'], # 簡易的にuser_idをセッションID代わりに使用
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
        st.warning(f"検索ログの保存に失敗しました: {e}")


def main_app():
    """
    認証後に表示されるメインアプリケーション
    """
    st.title("省庁資料検索ツール（Streamlit版）")
    
    # BigQueryクライアントを初期化
    try:
        bq_client = get_bigquery_client()
    except Exception as e:
        st.error(f"BigQueryクライアントの初期化に失敗しました: {e}")
        st.stop()

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

# セッションステートをチェックして、認証済みか判断
if not st.session_state['authenticated']:
    show_login_form()
else:
    main_app()