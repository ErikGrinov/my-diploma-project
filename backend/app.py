import os
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from fuzzywuzzy import fuzz, process
import tableauserverclient as TSC
import pantab as pt
from tableauhyperapi import TableName
import gc  # Для очищення пам'яті

# --- Налаштування Сервера ---
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "https://my-diploma-project.vercel.app"}})

# --- Стандартна Модель Даних ---
STANDARD_COLUMNS = {
    'Transaction_Date': ['дата', 'дата замовлення', 'date', 'order_date', 'time', 'datetime'],
    'Transaction_ID': ['id', 'номер замовлення', 'transaction id', 'order id', 'номер чека', 'ticket_number', 'ticket',
                       'receipt_id'],
    'Product_Category': ['категорія', 'category', 'product category', 'категорія товару', 'article', 'item_group'],
    'Quantity': ['кількість', 'quantity', 'qty', 'кіл-ть', 'pieces'],
    'Price_Per_Unit': ['ціна', 'price', 'ціна за од', 'unit_price', 'amount'],
    'Cost_Per_Unit': ['собівартість', 'cost', 'cost per unit'],
    'Client_Region': ['регіон', 'місто', 'region', 'city', 'client region', 'регіон доставки'],
}

# --- СХЕМА ДАНИХ ---
TABLEAU_SCHEMA = {
    'Transaction_Date': 'datetime64[ns]',
    'Transaction_ID': 'string',
    'Product_Category': 'string',
    'Quantity': 'float64',
    'Price_Per_Unit': 'float64',
    'Cost_Per_Unit': 'float64',
    'Client_Region': 'string',
    'Revenue': 'float64',
    'Profit': 'float64'
}

# --- СЛОВНИК МАРЖІ ---
MARGIN_FALLBACKS_BY_CATEGORY = {
    'Electronics': 0.20, 'Apparel': 0.40, 'Home Goods': 0.35,
    'Food': 0.15, 'Automotive': 0.10, 'Електроніка': 0.20,
    'Одяг': 0.40, 'Товари для дому': 0.35, 'Продукти': 0.15,
    'default': 0.30
}

CLEAN_CATEGORIES = list(MARGIN_FALLBACKS_BY_CATEGORY.keys())


def get_smart_category(dirty_category):
    if pd.isna(dirty_category) or str(dirty_category).strip() == "":
        return 'default'

    dirty_str = str(dirty_category)
    if dirty_str in MARGIN_FALLBACKS_BY_CATEGORY:
        return dirty_str

    best_match, score = process.extractOne(dirty_str.lower(), CLEAN_CATEGORIES, scorer=fuzz.token_set_ratio)
    if score > 60:
        return best_match
    else:
        return 'default'


def publish_to_tableau_cloud(file_path):
    try:
        server_url = os.environ['TABLEAU_SERVER_URL']
        site_id = os.environ['TABLEAU_SITE_ID']
        pat_name = os.environ['TABLEAU_PAT_NAME']
        pat_secret = os.environ['TABLEAU_PAT_SECRET']
        datasource_name_to_update = 'live_sales_data'

        print(f"Підключення до {server_url}...")
        tableau_auth = TSC.PersonalAccessTokenAuth(pat_name, pat_secret, site_id=site_id)
        server = TSC.Server(server_url, use_server_version=True)

        with server.auth.sign_in(tableau_auth):
            print("Вхід виконано.")
            req_option = TSC.RequestOptions()
            req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals,
                                             datasource_name_to_update))
            all_datasources, _ = server.datasources.get(req_option)

            if not all_datasources:
                return f"Помилка: Джерело '{datasource_name_to_update}' не знайдено."

            datasource_to_update = all_datasources[0]
            print(f"Публікую (ID: {datasource_to_update.id})...")
            server.datasources.publish(datasource_to_update, file_path, 'Overwrite')
            print(f"Успіх.")
            return None

    except TSC.ServerResponseError as e:
        return f"Помилка Tableau: {e.summary}"
    except Exception as e:
        return f"Помилка Python: {str(e)}"


def smart_column_mapping(uploaded_columns):
    mapping = {}
    used_standards = set()

    all_standard_options = []
    for standard_name, variations in STANDARD_COLUMNS.items():
        for var in variations:
            all_standard_options.append((var, standard_name))
    choices_dict = {opt[0]: opt[1] for opt in all_standard_options}
    choice_keys = list(choices_dict.keys())

    print(f"Вхідні стовпці: {uploaded_columns}")
    for col in uploaded_columns:
        clean_col = str(col).lower().strip().replace('_', ' ')
        if not clean_col: continue

        best_match, score = process.extractOne(clean_col, choice_keys, scorer=fuzz.token_sort_ratio)

        if score > 60:
            standard_name = choices_dict[best_match]
            if standard_name not in used_standards:
                mapping[col] = standard_name
                used_standards.add(standard_name)
                print(f"Знайдено: '{col}' -> '{standard_name}' ({score}%)")
    return mapping


# --- ФУНКЦІЯ ІНСАЙТІВ  ---
def generate_insights(df):
    insights = []
    try:
        # 1. Типізація
        df['Price_Per_Unit'] = pd.to_numeric(df['Price_Per_Unit'], errors='coerce')
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
        if 'Cost_Per_Unit' in df.columns:
            df['Cost_Per_Unit'] = pd.to_numeric(df['Cost_Per_Unit'], errors='coerce')

        df['Revenue'] = df['Price_Per_Unit'] * df['Quantity']

        # 2. Імп'ютація
        if 'Cost_Per_Unit' in df.columns:
            nan_count = df['Cost_Per_Unit'].isnull().sum()
            total_count = len(df)

            if nan_count == total_count:

                if 'Product_Category' in df.columns:
                    # Оптимізований розрахунок
                    unique_cats = df['Product_Category'].astype(str).unique()
                    cat_margin_map = {}
                    for cat in unique_cats:
                        smart_cat = get_smart_category(cat)
                        cat_margin_map[cat] = 1 - MARGIN_FALLBACKS_BY_CATEGORY.get(smart_cat, 0.30)

                    df.loc[:, 'Cost_Per_Unit'] = df['Price_Per_Unit'] * df['Product_Category'].astype(str).map(
                        cat_margin_map)


                    insights.append(
                        f"⚠️ **Увага:** Дані про собівартість товару були повністю відсутні. Прибуток розраховано **автоматично на основі галузевих стандартів маржі** для кожної категорії (наприклад, Електроніка: ~20%, Одяг: ~40%).")
                else:
                    df.loc[:, 'Cost_Per_Unit'] = df['Price_Per_Unit'] * 0.7
                    insights.append(
                        f"⚠️ **Увага:** Собівартість та Категорії відсутні. Для розрахунку прибутку була застосована **загальна теоретична маржа у 30%**.")

            elif nan_count > 0:

                good_data = df.dropna(subset=['Cost_Per_Unit', 'Price_Per_Unit'])
                if len(good_data) > 0:
                    avg_margin = (good_data['Price_Per_Unit'] - good_data['Cost_Per_Unit']).sum() / good_data[
                        'Price_Per_Unit'].sum()
                    if 0 < avg_margin < 1:
                        df.loc[df['Cost_Per_Unit'].isnull(), 'Cost_Per_Unit'] = df['Price_Per_Unit'] * (1 - avg_margin)
                        insights.append(
                            f"ℹ️ **Інформація:** {nan_count} транзакцій не мали собівартості. До них була автоматично застосована **середня розрахована маржа ({avg_margin:.1%})** з цього файлу.")
                    else:
                        df.loc[df['Cost_Per_Unit'].isnull(), 'Cost_Per_Unit'] = df['Price_Per_Unit'] * 0.7
                else:
                    df.loc[df['Cost_Per_Unit'].isnull(), 'Cost_Per_Unit'] = df['Price_Per_Unit'] * 0.7

        # 3. Розрахунок Profit
        if 'Cost_Per_Unit' in df.columns:
            df['Profit'] = df['Revenue'] - (df['Quantity'] * df['Cost_Per_Unit'])
        else:
            df['Profit'] = float('nan')

        # 4. Детальна Аналітика
        df_cleaned = df.dropna(subset=['Revenue'])
        total_revenue = df_cleaned['Revenue'].sum()
        total_transactions = df_cleaned['Transaction_ID'].nunique()
        insights.append(
            f"✅ Проаналізовано {total_transactions} унікальних транзакцій на загальну суму {total_revenue:,.2f} у.о.")

        aov = 0
        if total_transactions > 0:
            aov = total_revenue / total_transactions
            insights.append(f"📈 Середній чек (AOV) у цьому наборі даних становить {aov:,.2f} у.о.")

        # Топ категорія
        if 'Product_Category' in df_cleaned.columns:
            cat_group = df_cleaned.groupby('Product_Category')['Revenue'].sum().sort_values(ascending=False)
            if not cat_group.empty:
                top_cat = cat_group.idxmax()
                top_rev = cat_group.max()
                insights.append(f"🏆 Топ-категорія: '{top_cat}' з виручкою {top_rev:,.2f} у.о.")

        # Топ регіон
        if 'Client_Region' in df_cleaned.columns and df_cleaned['Client_Region'].notna().any():
            reg_group = df_cleaned.groupby('Client_Region')['Revenue'].sum().sort_values(ascending=False)
            if not reg_group.empty:
                top_reg = reg_group.idxmax()
                top_reg_rev = reg_group.max()
                insights.append(f"🌍 Топ-регіон: '{top_reg}' з виручкою {top_reg_rev:,.2f} у.о.")

        # 5. Розумні рекомендації
        if aov > 0:
            target_aov = aov * 1.15
            insights.append(
                f"💡 **Рекомендація:** Ваш середній чек {aov:,.2f} у.о. Спробуйте впровадити поріг безкоштовної доставки (наприклад, від {target_aov:,.2f} у.о.) або додайте 'cross-sell' товари, щоб заохотити клієнтів купувати більше.")

        if 'Product_Category' in df_cleaned.columns:
            cat_group = df_cleaned.groupby('Product_Category')['Revenue'].sum().sort_values(
                ascending=True)  # Сортуємо за зростанням
            if not cat_group.empty:
                worst_cat = cat_group.index[0]
                worst_rev = cat_group.iloc[0]
                insights.append(
                    f"📉 **Рекомендація:** Категорія '{worst_cat}' приносить найменше доходу ({worst_rev:,.2f} у.о.). Розгляньте можливість проведення цільової промо-акції для неї.")

        return insights

    except Exception as e:
        print(f"Помилка генерації інсайтів: {e}")
        return [f"Не вдалося згенерувати інсайти (помилка даних)."]


@app.route('/api/upload', methods=['POST'])
def upload_file():
    gc.collect()

    if 'file' not in request.files: return jsonify({"error": "Файл не знайдено"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"error": "Файл не обрано"}), 400

    if file and file.filename.endswith('.csv'):
        try:
            df = pd.read_csv(file)

            # Мапінг
            column_mapping = smart_column_mapping(df.columns.tolist())
            df.rename(columns=column_mapping, inplace=True)

            # Створення структури
            all_keys = list(TABLEAU_SCHEMA.keys())
            df_final = pd.DataFrame(index=df.index)

            for col in all_keys:
                if col in df.columns:
                    df_final[col] = df[col]
                else:
                    df_final[col] = None

            print("Застосовую типи даних...")
            if 'Transaction_Date' in df_final.columns:
                df_final['Transaction_Date'] = pd.to_datetime(df_final['Transaction_Date'], errors='coerce')

            for col, dtype in TABLEAU_SCHEMA.items():
                if dtype == 'string' or dtype == 'object':
                    df_final[col] = df_final[col].astype('string')
                elif col != 'Transaction_Date':
                    df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

            # Інсайти
            insights = generate_insights(df_final)

            # Запис
            temp_file_path = 'temp.hyper'
            print(f"Конвертую у {temp_file_path}...")

            if os.path.exists(temp_file_path): os.remove(temp_file_path)

            del df
            gc.collect()

            pt.frame_to_hyper(df_final, temp_file_path, table='Extract')

            print("Завантажую в Tableau...")
            tableau_error = publish_to_tableau_cloud(temp_file_path)

            if os.path.exists(temp_file_path): os.remove(temp_file_path)

            if tableau_error: insights.append(f"ПОМИЛКА TABLEAU: {tableau_error}")

            return jsonify({"message": "Успіх!", "insights": insights}), 200

        except Exception as e:
            return jsonify({"error": f"Помилка: {str(e)}"}), 500
    return jsonify({"error": "Тільки .csv"}), 400


if __name__ == '__main__':
    app.run()