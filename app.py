import streamlit as st
import pandas as pd
import json
import string
import random
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
if 'is_admin' not in st.session_state:
    st.session_state['is_admin'] = False
if 'show_admin_page' not in st.session_state:
    st.session_state['show_admin_page'] = False
if 'selected_agencies' not in st.session_state:
    st.session_state['selected_agencies'] = []
if 'selected_councils' not in st.session_state:
    st.session_state['selected_councils'] = []
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
    is_adminフラグも取得します。
    """
    auth_table_id_str = (
        f"`{st.secrets['bigquery']['project_id']}"
        f".{st.secrets['bigquery']['config_dataset']}"
        f".{st.secrets['bigquery']['auth_table']}`"
    )
    
    try:
        query = f"""
            SELECT id, IFNULL(is_admin, FALSE) as is_admin
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
        
        if not results.empty:
            is_admin = bool(results.iloc[0]['is_admin'])
            return True, is_admin
        return False, False
        
    except Exception as e:
        st.error(f"認証エラー: {e}")
        return False, False

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
                
                auth_result, is_admin = check_credentials_bigquery(bq_client, user_id, password)
                
                if auth_result:
                    st.session_state['authenticated'] = True
                    st.session_state['user_id'] = user_id
                    st.session_state['session_id'] = session_id
                    st.session_state['is_admin'] = is_admin
                    log_login_to_bigquery(bq_client, user_id, password, 'success', session_id)
                    st.rerun()
                else:
                    log_login_to_bigquery(bq_client, user_id, password, 'failed', session_id)
                    st.error("ユーザーIDまたはパスワードが間違っています。")

# ----------------------------------------------------------------------
# ユーティリティ関数
# ----------------------------------------------------------------------

def generate_password():
    """
    大文字・小文字・数字を必ず各1文字以上含む8文字のパスワードを生成します。
    """
    uppercase = random.choice(string.ascii_uppercase)
    lowercase = random.choice(string.ascii_lowercase)
    digit = random.choice(string.digits)
    
    remaining = ''.join(random.choices(
        string.ascii_uppercase + string.ascii_lowercase + string.digits,
        k=5
    ))
    
    password_list = list(uppercase + lowercase + digit + remaining)
    random.shuffle(password_list)
    
    return ''.join(password_list)

# ----------------------------------------------------------------------
# ユーザー管理機能
# ----------------------------------------------------------------------

def get_all_users(bq_client):
    """
    全ユーザー情報を取得します。
    """
    auth_table_id = (
        f"`{st.secrets['bigquery']['project_id']}"
        f".{st.secrets['bigquery']['config_dataset']}"
        f".{st.secrets['bigquery']['auth_table']}`"
    )
    
    try:
        query = f"""
            SELECT 
                id,
                code,
                pw,
                create_dt,
                update_dt,
                is_alive
            FROM {auth_table_id}
            WHERE is_admin = false or is_admin IS NULL
            ORDER BY create_dt DESC
        """
        
        df = bq_client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"ユーザー情報取得エラー: {e}")
        return pd.DataFrame()

def get_active_users(bq_client):
    """
    有効なユーザー情報を取得します。
    """
    auth_table_id = (
        f"`{st.secrets['bigquery']['project_id']}"
        f".{st.secrets['bigquery']['config_dataset']}"
        f".{st.secrets['bigquery']['auth_table']}`"
    )
    
    try:
        query = f"""
            SELECT 
                id,
                code,
                pw
            FROM {auth_table_id}
            WHERE is_alive = TRUE
            ORDER BY id
        """
        
        df = bq_client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"ユーザー情報取得エラー: {e}")
        return pd.DataFrame()

def check_user_exists(bq_client, user_id):
    """
    ユーザーIDが既に存在するかチェックします。
    """
    auth_table_id = (
        f"`{st.secrets['bigquery']['project_id']}"
        f".{st.secrets['bigquery']['config_dataset']}"
        f".{st.secrets['bigquery']['auth_table']}`"
    )
    
    try:
        query = f"""
            SELECT id
            FROM {auth_table_id}
            WHERE id = @user_id
            LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            ]
        )
        
        results = bq_client.query(query, job_config=job_config).to_dataframe()
        return not results.empty
    except Exception as e:
        st.error(f"ユーザー存在チェックエラー: {e}")
        return False

def insert_user(bq_client, user_id, code, password):
    """
    新規ユーザーを登録します。
    """
    auth_table_id = (
        f"{st.secrets['bigquery']['project_id']}"
        f".{st.secrets['bigquery']['config_dataset']}"
        f".{st.secrets['bigquery']['auth_table']}"
    )
    
    try:
        timestamp = pd.Timestamp.now(tz='Asia/Tokyo').isoformat()
        
        rows_to_insert = [
            {
                "id": user_id,
                "pw": password,
                "code": code,
                "is_alive": True,
                "is_admin": False,
                "create_dt": timestamp,
                "update_dt": timestamp
            }
        ]
        
        errors = bq_client.insert_rows_json(auth_table_id, rows_to_insert)
        
        if errors:
            st.error(f"ユーザー登録エラー: {errors}")
            return False
        return True
    except Exception as e:
        st.error(f"ユーザー登録エラー: {e}")
        return False

def update_user(bq_client, original_id, new_id, new_code, new_password):
    """
    ユーザー情報を更新します。
    """
    auth_table_id = (
        f"`{st.secrets['bigquery']['project_id']}"
        f".{st.secrets['bigquery']['config_dataset']}"
        f".{st.secrets['bigquery']['auth_table']}`"
    )
    
    try:
        update_fields = []
        params = [bigquery.ScalarQueryParameter("original_id", "STRING", original_id)]
        
        if new_id and new_id != original_id:
            update_fields.append("id = @new_id")
            params.append(bigquery.ScalarQueryParameter("new_id", "STRING", new_id))
        
        if new_code:
            update_fields.append("code = @new_code")
            params.append(bigquery.ScalarQueryParameter("new_code", "STRING", new_code))
        
        if new_password:
            update_fields.append("pw = @new_password")
            params.append(bigquery.ScalarQueryParameter("new_password", "STRING", new_password))
        
        timestamp = pd.Timestamp.now(tz='Asia/Tokyo').isoformat()
        update_fields.append("update_dt = @update_dt")
        params.append(bigquery.ScalarQueryParameter("update_dt", "TIMESTAMP", timestamp))
        
        if not update_fields:
            return True
        
        query = f"""
            UPDATE {auth_table_id}
            SET {', '.join(update_fields)}
            WHERE id = @original_id
        """
        
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        bq_client.query(query, job_config=job_config).result()
        
        return True
    except Exception as e:
        st.error(f"ユーザー更新エラー: {e}")
        return False

def delete_user(bq_client, user_id):
    """
    ユーザーを論理削除します（is_aliveをfalseに設定）。
    """
    auth_table_id = (
        f"`{st.secrets['bigquery']['project_id']}"
        f".{st.secrets['bigquery']['config_dataset']}"
        f".{st.secrets['bigquery']['auth_table']}`"
    )
    
    try:
        timestamp = pd.Timestamp.now(tz='Asia/Tokyo').isoformat()
        
        query = f"""
            UPDATE {auth_table_id}
            SET is_alive = FALSE,
                update_dt = @update_dt
            WHERE id = @user_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
                bigquery.ScalarQueryParameter("update_dt", "TIMESTAMP", timestamp)
            ]
        )
        
        bq_client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"ユーザー削除エラー: {e}")
        return False

# ----------------------------------------------------------------------
# モーダル関数
# ----------------------------------------------------------------------

@st.dialog("新規ユーザー登録", width="large")
def show_register_modal(bq_client):
    """
    新規ユーザー登録モーダル
    """
    if 'register_step' not in st.session_state:
        st.session_state['register_step'] = 'input'
    
    if st.session_state['register_step'] == 'input':
        st.markdown("ユーザーID・コードを入力してください")
        
        user_id = st.text_input("ユーザーID", key="register_user_id")
        code = st.text_input("コード", key="register_code")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("登録", type="primary", use_container_width=True):
                if not user_id or not code:
                    st.error("ユーザーIDとコードを入力してください")
                elif check_user_exists(bq_client, user_id):
                    st.error("入力されたユーザーIDは既に存在しています")
                else:
                    password = generate_password()
                    
                    if insert_user(bq_client, user_id, code, password):
                        st.session_state['register_step'] = 'complete'
                        st.session_state['registered_id'] = user_id
                        st.session_state['registered_code'] = code
                        st.session_state['registered_password'] = password
                        st.rerun()
        
        with col2:
            if st.button("キャンセル", use_container_width=True):
                st.session_state['register_step'] = 'input'
                st.rerun()
    
    elif st.session_state['register_step'] == 'complete':
        st.success("ユーザーを登録しました！")
        st.markdown("")
        st.markdown(f"**ユーザーID**: {st.session_state['registered_id']}")
        st.markdown(f"**コード**: {st.session_state['registered_code']}")
        st.markdown(f"**パスワード**: {st.session_state['registered_password']}")
        st.markdown("")
        
        if st.button("閉じる", use_container_width=True):
            st.session_state['register_step'] = 'input'
            del st.session_state['registered_id']
            del st.session_state['registered_code']
            del st.session_state['registered_password']
            st.rerun()

@st.dialog("ユーザー情報編集", width="large")
def show_edit_modal(bq_client):
    """
    ユーザー情報編集モーダル
    """
    if 'edit_step' not in st.session_state:
        st.session_state['edit_step'] = 'select'
    
    if st.session_state['edit_step'] == 'select':
        st.markdown("ユーザーIDを選択してください")
        
        active_users = get_active_users(bq_client)
        
        if active_users.empty:
            st.info("編集可能なユーザーがいません")
            if st.button("閉じる"):
                st.session_state['edit_step'] = 'select'
                st.rerun()
            return
        
        selected_user_id = st.selectbox(
            "ユーザーID",
            options=active_users['id'].tolist(),
            key="edit_select_user"
        )
        
        if selected_user_id:
            user_data = active_users[active_users['id'] == selected_user_id].iloc[0]
            
            st.markdown("---")
            st.markdown("**現在の内容**")
            st.text(f"ユーザーID: {selected_user_id}")
            st.text(f"コード: {user_data['code']}")
            st.text(f"パスワード: {user_data['pw']}")
            
            st.markdown("---")
            st.markdown("**新しい内容**（空欄の場合は更新しません）")
            
            new_id = st.text_input("ユーザーID", key="edit_new_id", placeholder=selected_user_id)
            new_code = st.text_input("コード", key="edit_new_code", placeholder=user_data['code'])
            new_password = st.text_input("パスワード", key="edit_new_password", placeholder=user_data['pw'])
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("更新", type="primary", use_container_width=True):
                    st.session_state['edit_step'] = 'confirm_update'
                    st.session_state['edit_user_id'] = selected_user_id
                    st.session_state['edit_user_code'] = user_data['code']
                    st.session_state['edit_user_pw'] = user_data['pw']
                    st.session_state['edit_new_id'] = new_id if new_id else selected_user_id
                    st.session_state['edit_new_code'] = new_code if new_code else user_data['code']
                    st.session_state['edit_new_password'] = new_password if new_password else user_data['pw']
                    st.rerun()
            
            with col2:
                if st.button("削除", use_container_width=True):
                    st.session_state['edit_step'] = 'confirm_delete'
                    st.session_state['edit_user_id'] = selected_user_id
                    st.session_state['edit_user_code'] = user_data['code']
                    st.rerun()
            
            with col3:
                if st.button("キャンセル", use_container_width=True):
                    st.session_state['edit_step'] = 'select'
                    st.rerun()
    
    elif st.session_state['edit_step'] == 'confirm_update':
        st.markdown("この内容で更新しますか？")
        st.markdown("")
        st.markdown(f"**ユーザーID**: {st.session_state['edit_new_id']}")
        st.markdown(f"**コード**: {st.session_state['edit_new_code']}")
        st.markdown(f"**パスワード**: {st.session_state['edit_new_password']}")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("OK", type="primary", use_container_width=True):
                if update_user(
                    bq_client,
                    st.session_state['edit_user_id'],
                    st.session_state['edit_new_id'],
                    st.session_state['edit_new_code'],
                    st.session_state['edit_new_password']
                ):
                    st.success("ユーザー情報を更新しました")
                    st.session_state['edit_step'] = 'select'
                    st.rerun()
        
        with col2:
            if st.button("キャンセル", use_container_width=True):
                st.session_state['edit_step'] = 'select'
                st.rerun()
    
    elif st.session_state['edit_step'] == 'confirm_delete':
        st.warning("本当にこのユーザーを削除しますか？")
        st.markdown("（この操作は元に戻せません）")
        st.markdown("")
        st.markdown(f"**ユーザーID**: {st.session_state['edit_user_id']}")
        st.markdown(f"**コード**: {st.session_state['edit_user_code']}")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("OK", type="primary", use_container_width=True):
                if delete_user(bq_client, st.session_state['edit_user_id']):
                    st.success("ユーザーを削除しました")
                    st.session_state['edit_step'] = 'select'
                    st.rerun()
        
        with col2:
            if st.button("キャンセル", use_container_width=True):
                st.session_state['edit_step'] = 'select'
                st.rerun()

# ----------------------------------------------------------------------
# ユーザー管理画面
# ----------------------------------------------------------------------

def show_admin_page(bq_client):
    """
    ユーザー管理画面
    """
    st.title("ユーザー管理")
    
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("新規登録", use_container_width=True):
            show_register_modal(bq_client)
    
    with col2:
        if st.button("編集", use_container_width=True):
            show_edit_modal(bq_client)
    
    with col3:
        if st.button("検索ツールに戻る", use_container_width=True):
            st.session_state['show_admin_page'] = False
            st.rerun()
    
    st.markdown("---")
    st.subheader("ユーザー一覧")
    
    users_df = get_all_users(bq_client)
    
    if not users_df.empty:
        display_df = users_df[users_df['is_alive'] == True][['id', 'code', 'pw', 'create_dt', 'update_dt']].copy()
        
        display_df['create_dt'] = pd.to_datetime(display_df['create_dt']).dt.strftime('%Y/%m/%d %H:%M')
        display_df['update_dt'] = pd.to_datetime(display_df['update_dt']).dt.strftime('%Y/%m/%d %H:%M')
        
        display_df.columns = ['ユーザーID', 'コード', 'パスワード', '登録日', '更新日']
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("ユーザーが登録されていません")

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

def run_search(_bq_client, dataset, table, column_names, keyword, agencies, councils, categories, sub_categories, years):
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

def log_search_to_bigquery(_bq_client, keyword, agencies, councils, categories, sub_categories, years):
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
    
    st.sidebar.header("🔽 条件絞り込み")
    
    st.sidebar.markdown("---")
    
    keyword = st.sidebar.text_input("**キーワード**", placeholder="キーワードを入力(複数の場合はスペースで区切る)")
    
    tree_data = load_ministry_tree()
    
    with st.sidebar:
        st.markdown("**省庁**")
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
    
    category_options = {item['title']: item['value'] for item in filter_choices['category']}
    selected_category_titles = st.sidebar.multiselect(
        "**カテゴリ**",
        options=list(category_options.keys())
    )
    categories = [category_options[title] for title in selected_category_titles]
    
    sub_category_options = {item['title']: item['value'] for item in filter_choices['sub_category']}
    selected_sub_category_titles = st.sidebar.multiselect(
        "**資料形式**",
        options=list(sub_category_options.keys())
    )
    sub_categories = [sub_category_options[title] for title in selected_sub_category_titles]
    
    year_options = {item['title']: item['value'] for item in filter_choices['year']}
    selected_year_titles = st.sidebar.multiselect(
        "**年度**",
        options=list(year_options.keys())
    )
    years = [year_options[title] for title in selected_year_titles]
    
    council_tree_data = load_council_list(bq_client)
    
    with st.sidebar:
        st.markdown("**会議体（会議資料のみ）**")
        if council_tree_data:
            council_result = st_ant_tree(
                treeData=council_tree_data,
                treeCheckable=True,
                allowClear=True,
                key="council_tree"
            )
            
            current_councils = extract_agencies_from_tree_result(council_result)
            st.session_state['selected_councils'] = current_councils
            
            if st.session_state['selected_councils']:
                st.caption(f"選択中: {len(st.session_state['selected_councils'])}件")
            else:
                st.caption("選択なし")
        else:
            st.info("会議体リストがありません")
    
    st.sidebar.markdown("---")
    
    search_button = st.sidebar.button("🔍 検索", type="primary", use_container_width=True)
    
    st.sidebar.markdown("")
    
    if st.sidebar.button("フィルタをリセット", use_container_width=True):
        st.session_state['selected_agencies'] = []
        st.session_state['selected_councils'] = []
        st.session_state['search_results'] = None
        st.rerun()
    
    st.sidebar.markdown("")
    
    if st.session_state['is_admin']:
        if st.sidebar.button("ユーザー管理", use_container_width=True):
            st.session_state['show_admin_page'] = True
            st.rerun()
        
        st.sidebar.markdown("")
    
    if st.sidebar.button("ログアウト", use_container_width=True):
        st.session_state['authenticated'] = False
        st.session_state['user_id'] = ""
        st.session_state['session_id'] = ""
        st.session_state['is_admin'] = False
        st.session_state['show_admin_page'] = False
        st.session_state['selected_agencies'] = []
        st.session_state['selected_councils'] = []
        st.session_state['search_results'] = None
        st.rerun()

    st.markdown("---")

    if search_button:
        agencies = st.session_state.get('selected_agencies', [])
        councils = st.session_state.get('selected_councils', [])
        
        log_search_to_bigquery(
            bq_client, keyword, agencies, councils, categories, 
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
                
                results_df = run_search(
                    bq_client, dataset, table, column_names,
                    keyword, agencies, councils_for_search, categories, sub_categories, years
                )
                all_results[tab_name] = {
                    "df": results_df,
                    "column_names": column_names
                }
            
            st.session_state['search_results'] = all_results
    
    tabs = st.tabs(["予算", "会議資料", "🔰使用方法・収録データ情報"])
    
    councils = st.session_state.get('selected_councils', [])
    
    with tabs[0]:
        if st.session_state['search_results'] is not None:
            if councils and len(councils) > 0:
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
    if st.session_state.get('show_admin_page', False) and st.session_state['is_admin']:
        show_admin_page(bq_client)
    else:
        main_app(bq_client)