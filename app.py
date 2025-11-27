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
            'title': '資料名',
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
            'title': '資料名',
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
if 'selected_councils' not in st.session_state:
    st.session_state['selected_councils'] = []
if 'selected_categories' not in st.session_state:
    st.session_state['selected_categories'] = []
if 'selected_sub_categories' not in st.session_state:
    st.session_state['selected_sub_categories'] = []
if 'selected_years' not in st.session_state:
    st.session_state['selected_years'] = []
if 'search_results' not in st.session_state:
    st.session_state['search_results'] = None

# 検索実行時の条件を保持するセッションステートを追加
if 'last_search_conditions' not in st.session_state:
    st.session_state['last_search_conditions'] = {
        'keyword_and': '',
        'keyword_or': '',
        'agencies': [],
        'councils': [],
        'categories': [],
        'sub_categories': [],
        'years': []
    }

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
            WHERE id = @user_id 
            AND pw = @password
            AND is_alive = TRUE
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
# JSONデータ読み込み
# ----------------------------------------------------------------------

@st.cache_data
def load_ministry_tree():
    """
    choices/ministry_tree.jsonを読み込みます。
    """
    file_path = Path(__file__).parent / "choices" / "ministry_tree.json"
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error(f"エラー: '{file_path}' が見つかりません。")
        return []
    except json.JSONDecodeError:
        st.error(f"エラー: '{file_path}' のJSON形式が不正です。")
        return []

@st.cache_data(ttl=3600)
def load_council_list(_bq_client):
    """
    BigQueryから会議体リストを読み込み、ツリー形式に変換します。
    """
    try:
        query = f"""
            SELECT 
                title,
                value,
                ministry
            FROM `{st.secrets["bigquery"]["project_id"]}.{st.secrets["bigquery"]["rawdata_dataset"]}.{st.secrets["bigquery"]["council_list"]}`
            ORDER BY ministry, title
        """
        
        df = _bq_client.query(query).to_dataframe()
        
        if df.empty:
            st.warning("会議体リストが空です")
            return []
        
        # ministryごとにグループ化してツリー形式に変換
        tree_data = []
        ministry_groups = df.groupby('ministry')
        
        for ministry, group in ministry_groups:
            children = [
                {"title": row['title'], "value": row['value']}
                for _, row in group.iterrows()
            ]
            
            tree_data.append({
                "title": ministry,
                "value": f"{ministry}_parent",
                "children": children
            })
        
        return tree_data
    except Exception as e:
        st.error(f"会議体リストの読み込みエラー: {e}")
        import traceback
        st.error(traceback.format_exc())
        return []

@st.cache_data
def load_filter_choices():
    """
    カテゴリ、資料形式、年度の選択肢をJSONファイルから読み込みます。
    """
    base_path = Path(__file__).parent / "choices"
    
    choices = {
        'category': [],
        'sub_category': [],
        'year': []
    }
    
    files = {
        'category': 'category.json',
        'sub_category': 'sub_category.json',
        'year': 'year.json'
    }
    
    for key, filename in files.items():
        file_path = base_path / filename
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                choices[key] = json.load(f)
        except FileNotFoundError:
            st.error(f"エラー: '{file_path}' が見つかりません。")
        except json.JSONDecodeError:
            st.error(f"エラー: '{file_path}' のJSON形式が不正です。")
    
    return choices

@st.cache_data
def load_manual():
    """
    マニュアルファイルを読み込みます。
    """
    manual_path = Path(__file__).parent / "docs" / "manual.md"
    try:
        with open(manual_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return f"マニュアルファイルが見つかりません: {manual_path}\n\ndocs/manual.md を作成してください。"

def extract_values_from_tree_result(tree_result):
    """
    st_ant_treeの結果から選択された値のリストを抽出します。
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

def run_search(_bq_client, dataset, table, column_names, keyword_and, keyword_or, agencies, councils, categories, sub_categories, years):
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
    
    if councils and len(councils) > 0:
        where_conditions.append("council IN UNNEST(@councils)")
        query_params.append(bigquery.ArrayQueryParameter("councils", "STRING", councils))
        
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

    # 【変更】キーワード検索条件の構築
    keyword_conditions = []
    
    # AND検索の条件
    if keyword_and:
        keywords = keyword_and.lower().split()
        for i, kw in enumerate(keywords):
            param_name = f"keyword_and_{i}"
            keyword_conditions.append(f"(LOWER(title) LIKE @{param_name} OR LOWER(content_text) LIKE @{param_name})")
            query_params.append(bigquery.ScalarQueryParameter(param_name, "STRING", f"%{kw}%"))

    # OR検索の条件
    if keyword_or:
        keywords = keyword_or.lower().split()
        or_sub_conditions = []
        for i, kw in enumerate(keywords):
            param_name = f"keyword_or_{i}"
            or_sub_conditions.append(f"(LOWER(title) LIKE @{param_name} OR LOWER(content_text) LIKE @{param_name})")
            query_params.append(bigquery.ScalarQueryParameter(param_name, "STRING", f"%{kw}%"))
        
        if or_sub_conditions:
            keyword_conditions.append("(" + " OR ".join(or_sub_conditions) + ")")

    if keyword_conditions:
        # AND/OR検索の条件全体を結合 (AND/OR検索をANDで結合)
        where_conditions.append(" AND ".join(keyword_conditions))

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

def log_search_to_bigquery(_bq_client, keyword_and, keyword_or, agencies, councils, categories, sub_categories, years):
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
                "keyword_and": keyword_and if keyword_and else "",
                "keyword_or": keyword_or if keyword_or else "",
                "filter_ministries": ", ".join(agencies) if agencies else "",
                "filter_councils": ", ".join(councils) if councils else "",
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
    
    filter_choices = load_filter_choices()
    
    st.sidebar.subheader("🔽 条件絞り込み")
    st.sidebar.markdown("> キーワード、省庁、カテゴリ、資料形式、年度、会議体で絞り込みが可能です。")
    st.sidebar.markdown("---")
    
    # 【変更】キーワード入力欄をAND/ORに分ける
    keyword_and = st.sidebar.text_input(
        "**キーワード (AND検索)**", 
        placeholder="例:AI 活用",
        help="複数の単語をスペースで区切ると、全ての単語を含む資料を検索します")

    keyword_or = st.sidebar.text_input(
        "**キーワード (OR検索)**", 
        placeholder="例:教育 医療",
        help="複数の単語をスペースで区切ると、いずれかの単語を含む資料を検索します")
    
    tree_data = load_ministry_tree()
    
    with st.sidebar:
        st.markdown("**省庁**", help="外局がある場合、管轄省庁を選択すると全て選択されます")
        if tree_data:
            tree_result = st_ant_tree(
                treeData=tree_data,
                treeCheckable=True,
                allowClear=True,
                showSearch=True,
                key="agency_tree"
            )
            
            current_agencies = extract_values_from_tree_result(tree_result)
            st.session_state['selected_agencies'] = current_agencies
        else:
            st.error("省庁ツリーの読み込みに失敗しました。")
    
    # カテゴリをツリー形式に変更
    with st.sidebar:
        st.markdown("**カテゴリ**", help="資料の大分類を選択できます")
        if filter_choices['category']:
            category_result = st_ant_tree(
                treeData=filter_choices['category'],
                treeCheckable=True,
                allowClear=True,
                showSearch=True,
                key="category_tree"
            )
            
            current_categories = extract_values_from_tree_result(category_result)
            st.session_state['selected_categories'] = current_categories
    
    # 資料形式をツリー形式に変更
    with st.sidebar:
        st.markdown("**資料形式**", help="資料の詳細な形式を選択できます")
        if filter_choices['sub_category']:
            sub_category_result = st_ant_tree(
                treeData=filter_choices['sub_category'],
                treeCheckable=True,
                allowClear=True,
                showSearch=True,
                key="sub_category_tree"
            )
            
            current_sub_categories = extract_values_from_tree_result(sub_category_result)
            st.session_state['selected_sub_categories'] = current_sub_categories

    
    # 年度をツリー形式に変更(フラットリストとして表示)
    with st.sidebar:
        st.markdown("**年度**", help="対象年度を選択できます(複数選択可)")
        if filter_choices['year']:
            year_result = st_ant_tree(
                treeData=filter_choices['year'],
                treeCheckable=True,
                allowClear=True,
                showSearch=True,
                key="year_tree"
            )
            
            current_years = extract_values_from_tree_result(year_result)
            st.session_state['selected_years'] = current_years
    
    council_tree_data = load_council_list(bq_client)
    
    with st.sidebar:
        st.markdown("**会議体(会議資料のみ)**", help="テキストを入力すると会議体名自体を絞り込み検索できます")
        if council_tree_data:
            council_result = st_ant_tree(
                treeData=council_tree_data,
                treeCheckable=True,
                allowClear=True,
                showSearch=True,
                key="council_tree"
            )
            
            current_councils = extract_values_from_tree_result(council_result)
            st.session_state['selected_councils'] = current_councils
        else:
            st.info("会議体リストがありません")
    
    st.sidebar.markdown("---")
    
    search_button = st.sidebar.button("🔍 検索", type="primary", use_container_width=True)
    
    st.sidebar.markdown("")
    
    if st.sidebar.button("フィルタをリセット", use_container_width=True):
        st.session_state['selected_agencies'] = []
        st.session_state['selected_councils'] = []
        st.session_state['selected_categories'] = []
        st.session_state['selected_sub_categories'] = []
        st.session_state['selected_years'] = []
        st.session_state['search_results'] = None
        st.rerun()
    
    st.sidebar.markdown("")
    
    if st.sidebar.button("ログアウト", use_container_width=True):
        st.session_state['authenticated'] = False
        st.session_state['user_id'] = ""
        st.session_state['session_id'] = ""
        st.session_state['selected_agencies'] = []
        st.session_state['selected_councils'] = []
        st.session_state['selected_categories'] = []
        st.session_state['selected_sub_categories'] = []
        st.session_state['selected_years'] = []
        st.session_state['search_results'] = None
        # last_search_conditions もリセット
        st.session_state['last_search_conditions'] = {
            'keyword_and': '', 'keyword_or': '', 'agencies': [], 'councils': [], 
            'categories': [], 'sub_categories': [], 'years': []
        }
        st.rerun()

    st.markdown("---")

    if search_button:
        agencies = st.session_state.get('selected_agencies', [])
        councils = st.session_state.get('selected_councils', [])
        categories = st.session_state.get('selected_categories', [])
        sub_categories = st.session_state.get('selected_sub_categories', [])
        years = st.session_state.get('selected_years', [])
        
        # 【追加】検索条件をセッションに保存
        st.session_state['last_search_conditions'] = {
            'keyword_and': keyword_and,
            'keyword_or': keyword_or,
            'agencies': agencies,
            'councils': councils,
            'categories': categories,
            'sub_categories': sub_categories,
            'years': years
        }
        
        # 【変更】ログ記録関数に新しい引数を渡す
        log_search_to_bigquery(
            bq_client, keyword_and, keyword_or, agencies, councils, categories, 
            sub_categories, years
        )
        
        with st.spinner("🔄 検索中..."):
            all_results = {}
            for tab_name, tab_config in TABLE_CONFIGS.items():
                if councils and len(councils) > 0 and tab_name == "予算":
                    all_results[tab_name] = {
                        "df": pd.DataFrame(),
                        "column_names": tab_config["columns"]
                    }
                    continue
                
                dataset = tab_config["dataset"]
                table = tab_config["table"]
                column_names = tab_config["columns"]
                
                councils_for_search = councils if tab_name == "会議資料" else []
                
                # 【変更】検索実行関数に新しい引数を渡す
                results_df = run_search(
                    bq_client, dataset, table, column_names,
                    keyword_and, keyword_or, agencies, councils_for_search, categories, sub_categories, years
                )
                all_results[tab_name] = {
                    "df": results_df,
                    "column_names": column_names
                }
            
            st.session_state['search_results'] = all_results
    
    # 【変更】検索条件の表示を 'last_search_conditions' に基づいて行う
    if st.session_state['search_results'] is not None:
        # 適用中の条件を last_search_conditions から取得
        last_conditions = st.session_state['last_search_conditions']
        
        keyword_and = last_conditions.get('keyword_and', '')
        keyword_or = last_conditions.get('keyword_or', '')
        agencies = last_conditions.get('agencies', [])
        councils = last_conditions.get('councils', [])
        categories = last_conditions.get('categories', [])
        sub_categories = last_conditions.get('sub_categories', [])
        years = last_conditions.get('years', [])
        
        search_conditions = ["📋 適用中の検索条件"]
        
        # キーワード (AND/ORを統合して表示)
        keyword_parts = []
        if keyword_and:
            keyword_parts.append(f"AND検索: **{keyword_and}**")
        if keyword_or:
            keyword_parts.append(f"OR検索: **{keyword_or}**")

        if keyword_parts:
            search_conditions.append(" / ".join(keyword_parts))
        
        # 省庁
        if agencies:
            if len(agencies) <= 3:
                search_conditions.append(f"**省庁**: {', '.join(agencies)}")
            else:
                search_conditions.append(f"**省庁**: {', '.join(agencies[:3])}... (計{len(agencies)}件)")
        
        # カテゴリ
        if categories:
            search_conditions.append(f"**カテゴリ**: {', '.join(categories)}")
        
        # 資料形式
        if sub_categories:
            if len(sub_categories) <= 3:
                search_conditions.append(f"**資料形式**: {', '.join(sub_categories)}")
            else:
                search_conditions.append(f"**資料形式**: {', '.join(sub_categories[:3])}... (計{len(sub_categories)}件)")
        
        # 年度
        if years:
            year_strs = [str(y) for y in sorted(years, reverse=True)]
            if len(year_strs) <= 5:
                search_conditions.append(f"**年度**: {', '.join(year_strs)}")
            else:
                search_conditions.append(f"**年度**: {', '.join(year_strs[:5])}... (計{len(year_strs)}件)")
        
        # 会議体
        if councils:
            if len(councils) <= 3:
                search_conditions.append(f"**会議体**: {', '.join(councils)}")
            else:
                search_conditions.append(f"**会議体**: {', '.join(councils[:3])}... (計{len(councils)}件)")
        
        if len(search_conditions) > 1:
            st.info(" | ".join(search_conditions))
        else:
            st.info("**条件**: すべての資料")
        
        st.markdown("---")
    
    tabs = st.tabs(["予算", "会議資料", "🔰使用方法・収録データ情報"])
    
    # councils は最新のフィルター選択肢を使用 (表示上の分岐のみ)
    councils_for_display = st.session_state.get('selected_councils', [])
    
    with tabs[0]:
        if st.session_state['search_results'] is not None:
            if councils_for_display and len(councils_for_display) > 0:
                st.info("会議体が選択されているため、予算の検索は実行されません。")
            else:
                results_df = st.session_state['search_results']["予算"]["df"]
                column_names = st.session_state['search_results']["予算"]["column_names"]
                
                if not results_df.empty:
                    page_count = len(results_df)
                    file_id_col_jp = column_names.get('file_id', 'ファイルID')
                    file_count = results_df[file_id_col_jp].nunique()
                    
                    st.success(f"{file_count}ファイル・{page_count}ページ ヒットしました")
                    
                    display_df = results_df.drop(columns=[file_id_col_jp])
                    
                    url_col_jp = column_names.get('source_url', 'URL')
                    if url_col_jp in display_df.columns:
                        st.dataframe(
                            display_df, 
                            height=2000, 
                            use_container_width=True,
                            column_config={
                                url_col_jp: st.column_config.LinkColumn(
                                    url_col_jp,
                                    display_text="📄リンク"
                                )
                            }
                        )
                    else:
                        st.dataframe(display_df, height=2000, use_container_width=True)
                else:
                    st.info("該当する結果が見つかりませんでした。")
        else:
            st.info("🔍 左側のサイドバーで条件を絞り込んで検索ボタンを押してください")
    
    with tabs[1]:
        if st.session_state['search_results'] is not None:
            results_df = st.session_state['search_results']["会議資料"]["df"]
            column_names = st.session_state['search_results']["会議資料"]["column_names"]
            
            if not results_df.empty:
                page_count = len(results_df)
                file_id_col_jp = column_names.get('file_id', 'ファイルID')
                file_count = results_df[file_id_col_jp].nunique()
                
                st.success(f"{file_count}ファイル・{page_count}ページ ヒットしました")
                
                display_df = results_df.drop(columns=[file_id_col_jp])
                
                url_col_jp = column_names.get('source_url', 'URL')
                if url_col_jp in display_df.columns:
                    st.dataframe(
                        display_df, 
                        height=2000, 
                        use_container_width=True,
                        column_config={
                            url_col_jp: st.column_config.LinkColumn(
                                url_col_jp,
                                display_text="📄リンク"
                            )
                        }
                    )
                else:
                    st.dataframe(display_df, height=2000, use_container_width=True)
            else:
                st.info("該当する結果が見つかりませんでした。")
        else:
            st.info("🔍 左側のサイドバーで条件を絞り込んで検索ボタンを押してください")
    
    with tabs[2]:
        manual_content = load_manual()
        st.markdown(manual_content)

# ----------------------------------------------------------------------
# アプリケーションの実行
# ----------------------------------------------------------------------

bq_client = get_bigquery_client()

if not st.session_state['authenticated']:
    show_login_form(bq_client)
else:
    main_app(bq_client)