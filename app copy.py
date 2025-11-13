import streamlit as st
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from st_ant_tree import st_ant_tree

# ----------------------------------------------------------------------
# ページ設定
# ----------------------------------------------------------------------
st.set_page_config(
    page_title="省庁資料検索ツール (β版_v2)",
    layout="wide"
)

# ----------------------------------------------------------------------
# テーブル設定(各タブ用)
# ----------------------------------------------------------------------
TABLE_CONFIGS = {
    "予算": {
        "dataset": st.secrets["bigquery"]["rawdata_dataset"],
        "table": st.secrets["bigquery"]["budget_table"],
        "columns": {
            'file_id': 'ファイルID',
            'title': 'タイトル',
            'ministry': '省庁',
            'agency': '本局/外局',
            'fiscal_year_start': '年度',
            'category': 'カテゴリ',
            'sub_category': '資料形式',
            'file_page': 'ページ',
            'source_url': 'URL',
            'content_text': '本文'
        }
    },
    "会議資料": {
        "dataset": st.secrets["bigquery"]["rawdata_dataset"],
        "table": st.secrets["bigquery"]["council_table"],
        "columns": {
            'file_id': 'ファイルID',
            'title': 'タイトル',
            'ministry': '省庁',
            'agency': '本局/外局',
            'council': '会議体名',
            'fiscal_year_start': '年度',
            'category': 'カテゴリ',
            'sub_category': '資料形式',
            'file_page': 'ページ',
            'source_url': 'URL',
            'content_text': '本文'
        }
    }
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
# セッション管理
# ----------------------------------------------------------------------

def generate_session_id(user_id):
    """
    セッションIDを生成します。
    形式: ログインID_YYYYMMDDhhmmssss
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    return f"{user_id}_{timestamp}"

if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
if 'user_id' not in st.session_state:
    st.session_state['user_id'] = ""
if 'session_id' not in st.session_state:
    st.session_state['session_id'] = ""
if 'selected_agencies' not in st.session_state:
    st.session_state['selected_agencies'] = []
if 'search_results' not in st.session_state:
    st.session_state['search_results'] = None

# ----------------------------------------------------------------------
# 認証
# ----------------------------------------------------------------------

def log_login_to_bigquery(_bq_client, input_user_id, input_password, login_result, session_id):
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
                "sessionId": session_id
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
    st.title("省庁資料検索ツール (β版_v2) - ログイン")
    
    with st.form("login_form"):
        user_id = st.text_input("ユーザーID")
        password = st.text_input("パスワード", type="password")
        submitted = st.form_submit_button("ログイン")

        if submitted:
            if not user_id or not password:
                st.error("ユーザーIDとパスワードを入力してください。")
                return

            with st.spinner("認証中..."):
                session_id = generate_session_id(user_id)
                
                if check_credentials_bigquery(bq_client, user_id, password):
                    st.session_state['authenticated'] = True
                    st.session_state['user_id'] = user_id
                    st.session_state['session_id'] = session_id
                    log_login_to_bigquery(bq_client, user_id, password, 'success', session_id)
                    st.rerun()
                else:
                    log_login_to_bigquery(bq_client, user_id, password, 'failed', session_id)
                    st.error("ユーザーIDまたはパスワードが間違っています。")

# ----------------------------------------------------------------------
# ツリーデータ読み込み
# ----------------------------------------------------------------------

@st.cache_data
def load_ministry_tree():
    """
    ministry_tree.jsonを読み込みます。
    """
    file_path = Path(__file__).parent / "ministry_tree.json"
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error(f"エラー: '{file_path.name}' が見つかりません。")
        return []
    except json.JSONDecodeError:
        st.error(f"エラー: '{file_path.name}' のJSON形式が不正です。")
        return []

def extract_agencies_from_tree_result(tree_result):
    """
    st_ant_treeの結果から選択された本局/外局名のリストを抽出します。
    """
    if not tree_result:
        return []
    
    if isinstance(tree_result, list):
        return tree_result
    
    if isinstance(tree_result, dict):
        if 'checked' in tree_result:
            return tree_result['checked'] if isinstance(tree_result['checked'], list) else []
    
    return []

# ----------------------------------------------------------------------
# メインアプリケーション
# ----------------------------------------------------------------------

@st.cache_data(ttl=3600)
def load_metadata(_bq_client, dataset, table):
    """
    フィルタ用のメタデータをBigQueryから読み込みます。
    """
    query = f"""
      SELECT 
        ministry,
        agency,
        category,
        sub_category,
        fiscal_year_start
      FROM `{st.secrets["bigquery"]["project_id"]}.{dataset}.{table}`
      GROUP BY ministry, agency, category, sub_category, fiscal_year_start
      ORDER BY ministry, agency, category, sub_category, fiscal_year_start
    """
    try:
        df = _bq_client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"メタデータの読み込みエラー: {e}")
        return pd.DataFrame()

def run_search(_bq_client, dataset, table, column_names, keyword, agencies, categories, sub_categories, years):
    """
    検索クエリを実行します。
    """
    db_columns = list(column_names.keys())
    columns_str = ", ".join(db_columns)
    
    base_query = f"""
        SELECT 
            {columns_str}
        FROM `{st.secrets["bigquery"]["project_id"]}.{dataset}.{table}`
    """
    
    where_conditions = []
    query_params = []

    if agencies and len(agencies) > 0:
        where_conditions.append("agency IN UNNEST(@agencies)")
        query_params.append(bigquery.ArrayQueryParameter("agencies", "STRING", agencies))
        
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
        
    final_query += " ORDER BY ministry, agency, category, fiscal_year_start"

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    
    try:
        df = _bq_client.query(final_query, job_config=job_config).to_dataframe()
        df = df.rename(columns=column_names)
        return df
    except Exception as e:
        st.error(f"検索エラー: {e}")
        return pd.DataFrame()

def log_search_to_bigquery(_bq_client, keyword, agencies, categories, sub_categories, years):
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
                "sessionId": st.session_state['session_id'],
                "keyword": keyword if keyword else "",
                "filter_ministries": ", ".join(agencies) if agencies else "",
                "filter_category": ", ".join(categories) if categories else "",
                "filter_subcategory": ", ".join(sub_categories) if sub_categories else "",
                "filter_year": ", ".join([str(y) for y in years]) if years else ""
            }
        ]
        
        errors = _bq_client.insert_rows_json(log_table_id, rows_to_insert)
        if errors:
            st.warning(f"検索ログ記録エラー: {errors}")
    except Exception as e:
        st.warning(f"検索ログ記録エラー: {e}")

def main_app(bq_client):
    """
    認証後に表示されるメインアプリケーション
    """
    st.title("省庁資料検索ツール (β版_v2)")
    
    # サイドバー (フィルタ)
    st.sidebar.header("🔽 条件絞り込み")
    
    keyword = st.sidebar.text_input("キーワード", placeholder="キーワードを入力")
    
    # ツリー形式の省庁選択
    tree_data = load_ministry_tree()
    
    with st.sidebar:
        st.markdown("省庁")
        if tree_data:
            tree_result = st_ant_tree(
                treeData=tree_data,
                treeCheckable=True,
                allowClear=True,
                key="agency_tree"
            )
            
            current_agencies = extract_agencies_from_tree_result(tree_result)
            st.session_state['selected_agencies'] = current_agencies
            
            if st.session_state['selected_agencies']:
                st.caption(f"選択中: {', '.join(st.session_state['selected_agencies'])}")
            else:
                st.caption("選択なし")
        else:
            st.error("省庁ツリーの読み込みに失敗しました。")
    
    # 全テーブルのメタデータを統合して読み込み
    with st.spinner("フィルタを読み込み中..."):
        all_meta_dfs = []
        for tab_name, tab_config in TABLE_CONFIGS.items():
            meta_df = load_metadata(bq_client, tab_config["dataset"], tab_config["table"])
            if not meta_df.empty:
                all_meta_dfs.append(meta_df)
        
        if all_meta_dfs:
            combined_meta_df = pd.concat(all_meta_dfs, ignore_index=True).drop_duplicates()
        else:
            st.sidebar.error("フィルタの読み込みに失敗しました。")
            st.stop()

    categories = st.sidebar.multiselect(
        "カテゴリ:",
        sorted(combined_meta_df['category'].unique())
    )
    sub_categories = st.sidebar.multiselect(
        "資料形式:",
        sorted(combined_meta_df['sub_category'].unique())
    )
    years = st.sidebar.multiselect(
        "年度:",
        sorted(combined_meta_df['fiscal_year_start'].unique(), reverse=True)
    )

    st.sidebar.markdown("---")
    
    search_button = st.sidebar.button("🔍 検索", type="primary", use_container_width=True)
    
    st.sidebar.markdown("")
    
    if st.sidebar.button("フィルタをリセット", use_container_width=True):
        st.session_state['selected_agencies'] = []
        st.session_state['search_results'] = None
        st.rerun()
    
    st.sidebar.markdown("")
    
    if st.sidebar.button("ログアウト", use_container_width=True):
        st.session_state['authenticated'] = False
        st.session_state['user_id'] = ""
        st.session_state['session_id'] = ""
        st.session_state['selected_agencies'] = []
        st.session_state['search_results'] = None
        st.rerun()

    # メインコンテンツ (検索結果をタブで表示)
    st.markdown("---")

    if search_button:
        agencies = st.session_state.get('selected_agencies', [])
        
        # 検索ログを記録（検索実行時に1回だけ）
        log_search_to_bigquery(
            bq_client, keyword, agencies, categories, 
            sub_categories, years
        )
        
        with st.spinner("🔄 検索中..."):
            all_results = {}
            for tab_name, tab_config in TABLE_CONFIGS.items():
                dataset = tab_config["dataset"]
                table = tab_config["table"]
                column_names = tab_config["columns"]
                
                results_df = run_search(
                    bq_client, dataset, table, column_names,
                    keyword, agencies, categories, sub_categories, years
                )
                all_results[tab_name] = {
                    "df": results_df,
                    "column_names": column_names
                }
            
            # 検索結果をセッションステートに保存
            st.session_state['search_results'] = all_results
    
    # 検索結果の表示（セッションステートから取得）
    if st.session_state['search_results'] is not None:
        all_results = st.session_state['search_results']
        tabs = st.tabs(list(TABLE_CONFIGS.keys()))
        
        for i, (tab_name, tab) in enumerate(zip(TABLE_CONFIGS.keys(), tabs)):
            with tab:
                results_df = all_results[tab_name]["df"]
                column_names = all_results[tab_name]["column_names"]
                
                if not results_df.empty:
                    page_count = len(results_df)
                    file_id_col = column_names.get('file_id', 'file_id')
                    file_count = results_df[file_id_col].nunique()
                    
                    st.success(f"{file_count}ファイル・{page_count}ページ ヒットしました")
                    
                    url_col = column_names.get('source_url')
                    if url_col:
                        st.dataframe(
                            results_df, 
                            height=2000, 
                            use_container_width=True,
                            column_config={
                                url_col: st.column_config.LinkColumn(
                                    url_col,
                                    display_text="📄リンク"
                                )
                            }
                        )
                    else:
                        st.dataframe(results_df, height=2000, use_container_width=True)
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