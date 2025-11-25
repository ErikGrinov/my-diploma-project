import os
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from fuzzywuzzy import fuzz, process
import tableauserverclient as TSC
import pantab as pt
from tableauhyperapi import TableName

# --- Налаштування Сервера ---
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "https://my-diploma-project.vercel.app"}})

# --- Стандартна Модель Даних ---
STANDARD_COLUMNS = {
    'Transaction_Date': ['дата', 'дата замовлення', 'date', 'order_date', 'time'],
    'Transaction_ID': ['id', 'номер замовлення', 'transaction id', 'order id', 'номер чека', 'ticket_number'],
    'Product_Category': ['категорія', 'category', 'product category', 'категорія товару', 'article'],
    'Quantity': ['кількість', 'quantity', 'qty', 'кіл-ть'],
    'Price_Per_Unit': ['ціна', 'price', 'ціна за од', 'unit_price'],
    'Cost_Per_Unit': ['собівартість', 'cost', 'cost per unit'],
    'Client_Region': ['регіон', 'місто', 'region', 'city', 'client region', 'регіон доставки'],
}

# --- СХЕМА ДАНИХ ---
TABLEAU_SCHEMA = {
    'Transaction_Date': 'datetime64[ns]',
    'Transaction_ID': 'object',
    'Product_Category': 'object',
    'Quantity': 'Int64',
    'Price_Per_Unit': 'float64',
    'Cost_Per_Unit': 'float64',
    'Client_Region': 'object'
}

# --- СЛОВНИК МАРЖІ ---
MARGIN_FALLBACKS_BY_CATEGORY = {
    'Electronics': 0.20,
    'Apparel': 0.40,
    'Home Goods': 0.35,
    'Food': 0.15,
    'Automotive': 0.10,
    'Електроніка': 0.20,
    'Одяг': 0.40,
    'Товари для дому': 0.35,
    'Продукти': 0.15,
    'default': 0.30
}

# --- РОЗУМНА КАТЕГОРІЯ ---
CLEAN_CATEGORIES = list(MARGIN_FALLBACKS_BY_CATEGORY.keys())


def get_smart_category(dirty_category):
    """Знаходить найкращий збіг для категорії."""
    if pd.isna(dirty_category) or str(dirty_category).strip() == "":
        return 'default'

    # Оптимізація: якщо точний збіг, не запускаємо fuzzy
    if dirty_category in MARGIN_FALLBACKS_BY_CATEGORY:
        return dirty_category

    best_match, score = process.extractOne(
        str(dirty_category).lower(),
        CLEAN_CATEGORIES,
        scorer=fuzz.token_set_ratio
    )
    if score > 60:
        return best_match
    else:
        return 'default'


# --- Функція Публікації ---
def publish_to_tableau_cloud(file_path):
    try:
        server_url = os.environ['TABLEAU_SERVER_URL']
        site_id = os.environ['TABLEAU_SITE_ID']
        pat_name = os.environ['TABLEAU_PAT_NAME']
        pat_secret = os.environ['TABLEAU_PAT_SECRET']
        datasource_name_to_update = 'live_sales_data'

        print(f"Підключення до {server_url} на сайті {site_id}...")
        tableau_auth = TSC.PersonalAccessTokenAuth(pat_name, pat_secret, site_id=site_id)
        server = TSC.Server(server_url, use_server_version=True)

        with server.auth.sign_in(tableau_auth):
            print("Успішний вхід в Tableau Cloud.")
            req_option = TSC.RequestOptions()
            req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                             TSC.RequestOptions.Operator.Equals,
                                             datasource_name_to_update))
            all_datasources, _ = server.datasources.get(req_option)

            if not all_datasources:
                return f"Помилка: Джерело '{datasource_name_to_update}' не знайдено."

            datasource_to_update = all_datasources[0]
            print(f"Публікую нову версію...")
            server.datasources.publish(datasource_to_update, file_path, 'Overwrite')
            print(f"Джерело даних успішно оновлено.")
            return None

    except TSC.ServerResponseError as e:
        return f"Помилка Tableau API: {e.summary} - {e.detail}"
    except Exception as e:
        return f"Критична помилка Python: {str(e)}"


# --- Функція Мапінгу ---
def smart_column_mapping(uploaded_columns):
    mapping = {}
    used_standards = set()  # Щоб уникнути дублікатів (напр., date і time -> Transaction_Date)

    all_standard_options = []
    for standard_name, variations in STANDARD_COLUMNS.items():
        for var in variations:
            all_standard_options.append((var, standard_name))
    choices_dict = {opt[0]: opt[1] for opt in all_standard_options}
    choice_keys = list(choices_dict.keys())

    print(f"Вхідні стовпці: {uploaded_columns}")
    for col in uploaded_columns:
        clean_col = str(col).lower().strip().replace('_', ' ')
        best_match, score = process.extractOne(clean_col, choice_keys, scorer=fuzz.token_sort_ratio)

        if score > 60:
            standard_name = choices_dict[best_match]
            if standard_name not in used_standards:
                mapping[col] = standard_name
                used_standards.add(standard_name)
                print(f"Знайдено: '{col}' -> '{standard_name}' ({score}%)")
        else:
            print(f"НЕ знайдено: '{col}'")
    return mapping


# --- ОПТИМІЗОВАНА ФУНКЦІЯ ІНСАЙТІВ ---
def generate_insights(df):
    insights = []
    try:
        # 1. Типізація
        df['Price_Per_Unit'] = pd.to_numeric(df['Price_Per_Unit'], errors='coerce')
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
        if 'Cost_Per_Unit' in df.columns:
            df['Cost_Per_Unit'] = pd.to_numeric(df['Cost_Per_Unit'], errors='coerce')

        df['Revenue'] = df['Price_Per_Unit'] * df['Quantity']

        # 2. ОПТИМІЗОВАНА РОЗУМНА ІМП'ЮТАЦІЯ
        if 'Cost_Per_Unit' in df.columns:
            nan_count = df['Cost_Per_Unit'].isnull().sum()
            total_count = len(df)

            if nan_count == total_count:
                # СЦЕНАРІЙ Б: Стовпець повністю порожній
                if 'Product_Category' not in df.columns:
                    fallback_margin = MARGIN_FALLBACKS_BY_CATEGORY['default']
                    df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * (1 - fallback_margin), inplace=True)
                    insights.append(
                        f"⚠️ **Увага:** Собівартість ТА Категорії відсутні. Застосовано маржу {fallback_margin:.0%}.")
                else:
                    print("Імп'ютація: Оптимізований розрахунок собівартості...")

                    # --- ОПТИМІЗАЦІЯ: Рахуємо лише для унікальних категорій ---
                    unique_cats = df['Product_Category'].astype(str).unique()

                    # Створюємо мапу: { 'Назва в файлі': Коефіцієнт собівартості }
                    cat_multiplier_map = {}
                    for cat in unique_cats:
                        smart_cat = get_smart_category(cat)  # Викликаємо важку функцію лише 1 раз на категорію
                        margin = MARGIN_FALLBACKS_BY_CATEGORY.get(smart_cat, MARGIN_FALLBACKS_BY_CATEGORY['default'])
                        cat_multiplier_map[cat] = 1 - margin

                    # Застосовуємо мапу миттєво до всього файлу
                    df['Cost_Per_Unit'] = df['Price_Per_Unit'] * df['Product_Category'].astype(str).map(
                        cat_multiplier_map)

                    insights.append(
                        f"⚠️ **Увага:** Собівартість була відсутня. Прибуток розраховано на основі 'розумного' зіставлення категорій.")

            elif nan_count > 0:
                # СЦЕНАРІЙ A: Частково відсутній (середнє)
                good_data = df.dropna(subset=['Cost_Per_Unit', 'Price_Per_Unit'])
                if len(good_data) > 0:
                    avg_margin = (good_data['Price_Per_Unit'] - good_data['Cost_Per_Unit']).sum() / good_data[
                        'Price_Per_Unit'].sum()
                    if 0 < avg_margin < 1:
                        df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * (1 - avg_margin), inplace=True)
                        insights.append(
                            f"ℹ️ **Інформація:** Застосовано середню маржу ({avg_margin:.1%}) для пропущених записів.")
                    else:
                        df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * 0.7, inplace=True)
                else:
                    df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * 0.7, inplace=True)

        # 3. Розрахунок Profit
        if 'Cost_Per_Unit' in df.columns:
            df['Profit'] = df['Revenue'] - (df['Quantity'] * df['Cost_Per_Unit'])
        else:
            df['Profit'] = float('nan')

        # 4. Аналітика
        df_cleaned = df.dropna(subset=['Revenue'])
        total_revenue = df_cleaned['Revenue'].sum()
        total_transactions = df_cleaned['Transaction_ID'].nunique()
        insights.append(f"✅ Проаналізовано {total_transactions} транзакцій на суму {total_revenue:,.2f} грн.")

        aov = 0
        if total_transactions > 0:
            aov = total_revenue / total_transactions
            insights.append(f"📈 Середній чек (AOV): {aov:,.2f} грн.")

        # Топ категорія (спрощено для швидкості)
        if 'Product_Category' in df_cleaned.columns:
            cat_group = df_cleaned.groupby('Product_Category')['Revenue'].sum().sort_values(ascending=False)
            if not cat_group.empty:
                insights.append(f"🏆 Топ-категорія: '{cat_group.idxmax()}' ({cat_group.max():,.2f} грн).")

        if 'Client_Region' in df_cleaned.columns and df_cleaned['Client_Region'].notna().any():
            reg_group = df_cleaned.groupby('Client_Region')['Revenue'].sum().sort_values(ascending=False)
            if not reg_group.empty:
                insights.append(f"🌍 Топ-регіон: '{reg_group.idxmax()}' ({reg_group.max():,.2f} грн).")

        if aov > 0:
            target_aov = aov * 1.15
            insights.append(f"💡 **Рекомендація:** Підніміть середній чек до {target_aov:,.2f} грн.")

        return insights

    except Exception as e:
        print(f"Помилка генерації інсайтів: {e}")
        return [f"Не вдалося згенерувати інсайти (помилка даних)."]


# --- API ENDPOINT ---
@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "Файл не знайдено"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Файл не обрано"}), 400

    if file and file.filename.endswith('.csv'):
        try:
            df = pd.read_csv(file)
            column_mapping = smart_column_mapping(df.columns.tolist())
            df.rename(columns=column_mapping, inplace=True)

            all_standard_keys = list(STANDARD_COLUMNS.keys())
            df_standard = pd.DataFrame(columns=all_standard_keys)
            final_columns = [col for col in all_standard_keys if col in df.columns]
            df_final = pd.concat([df_standard, df[final_columns]], sort=False)

            print("Примусово застосовую типи даних...")
            try:
                if 'Transaction_Date' in df_final.columns:
                    df_final['Transaction_Date'] = pd.to_datetime(df_final['Transaction_Date'], errors='coerce')
                df_final = df_final.astype(TABLEAU_SCHEMA, errors='ignore')
            except Exception as e:
                print(f"!! Помилка типів: {e}")

            insights = generate_insights(df_final)

            temp_file_path = os.path.join('temp_cleaned_data.hyper')
            print(f"Конвертую дані у {temp_file_path}...")
            pt.frame_to_hyper(df_final, temp_file_path, table='Extract')

            print("Запускаю оновлення даних в Tableau Cloud...")
            tableau_error = publish_to_tableau_cloud(temp_file_path)

            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

            if tableau_error:
                insights.append(f"ПОМИЛКА TABLEAU: {tableau_error}")

            return jsonify({
                "message": "Файл успішно оброблено!",
                "insights": insights
            }), 200

        except Exception as e:
            return jsonify({"error": f"Помилка обробки: {str(e)}"}), 500
    else:
        return jsonify({"error": "Невірний тип файлу. Потрібен .csv"}), 400


if __name__ == '__main__':
    app.run()