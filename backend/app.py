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
    # time теж тут, але ми візьмемо лише перше співпадіння
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
    if not isinstance(dirty_category, str):
        return 'default'
    best_match, score = process.extractOne(dirty_category.lower(), CLEAN_CATEGORIES, scorer=fuzz.token_set_ratio)
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
                error_msg = f"Помилка: Джерело даних з ім'ям '{datasource_name_to_update}' не знайдено."
                print(f"!! {error_msg}")
                return error_msg

            datasource_to_update = all_datasources[0]
            print(f"Джерело даних знайдено (ID: {datasource_to_update.id}). Публікую нову версію...")

            updated_datasource = server.datasources.publish(datasource_to_update, file_path, 'Overwrite')
            print(f"Джерело даних '{updated_datasource.name}' успішно оновлено.")

            return None

    except TSC.ServerResponseError as e:
        error_msg = f"Помилка Tableau API: {e.summary} - {e.detail}"
        print(f"!! {error_msg}")
        return error_msg
    except Exception as e:
        error_msg = f"Критична помилка Python: {str(e)}"
        print(f"!! {error_msg}")
        return error_msg


# --- ↓↓↓ ВИПРАВЛЕНА ФУНКЦІЯ МАПІНГУ (ЗАБОРОНЯЄ ДУБЛІКАТИ) ↓↓↓ ---
def smart_column_mapping(uploaded_columns):
    mapping = {}

    # Множина для відстеження, які стандартні стовпці ми вже знайшли
    used_standard_cols = set()

    all_standard_options = []
    for standard_name, variations in STANDARD_COLUMNS.items():
        for var in variations:
            all_standard_options.append((var, standard_name))
    choices_dict = {opt[0]: opt[1] for opt in all_standard_options}
    choice_keys = list(choices_dict.keys())

    print(f"Вхідні стовпці: {uploaded_columns}")

    for col in uploaded_columns:
        clean_col = str(col).lower().strip().replace('_', ' ')

        # Використовуємо token_sort_ratio для кращого співпадіння
        best_match, score = process.extractOne(clean_col, choice_keys, scorer=fuzz.token_sort_ratio)

        if score > 60:
            standard_name = choices_dict[best_match]

            # --- ПЕРЕВІРКА НА ДУБЛІКАТИ ---
            if standard_name in used_standard_cols:
                print(
                    f"ПРОПУСКАЮ: Стовпець '{col}' схожий на '{standard_name}', але цей стандартний стовпець вже зайнятий.")
                continue

            mapping[col] = standard_name
            used_standard_cols.add(standard_name)  # Позначаємо як зайнятий

            print(f"Знайдено: '{col}' -> '{standard_name}' (Схожість: {score}%)")
        else:
            print(f"НЕ знайдено: '{col}' (Найкращий варіант: '{best_match}' з {score}%)")

    return mapping


# --- ФУНКЦІЯ ІНСАЙТІВ ---
def generate_insights(df):
    insights = []
    try:
        df['Price_Per_Unit'] = pd.to_numeric(df['Price_Per_Unit'], errors='coerce')
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
        if 'Cost_Per_Unit' in df.columns:
            df['Cost_Per_Unit'] = pd.to_numeric(df['Cost_Per_Unit'], errors='coerce')

        df['Revenue'] = df['Price_Per_Unit'] * df['Quantity']

        if 'Cost_Per_Unit' in df.columns:
            nan_count = df['Cost_Per_Unit'].isnull().sum()
            total_count = len(df)
            if nan_count == total_count:
                if 'Product_Category' not in df.columns:
                    fallback_margin = MARGIN_FALLBACKS_BY_CATEGORY['default']
                    df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * (1 - fallback_margin), inplace=True)
                    insights.append(
                        f"⚠️ **Увага:** Собівартість (`Cost_Per_Unit`) ТА Категорії відсутні. Застосовано заг. маржу {fallback_margin:.0%}.")
                else:
                    print("Імп'ютація: Cost_Per_Unit повністю відсутній. Застосовую 'розумну' маржу...")
                    df['Cost_Per_Unit'] = df.apply(
                        lambda row: row['Price_Per_Unit'] * (
                                    1 - MARGIN_FALLBACKS_BY_CATEGORY[get_smart_category(row['Product_Category'])]),
                        axis=1
                    )
                    insights.append(
                        f"⚠️ **Увага:** Собівартість (`Cost_Per_Unit`) була відсутня. Прибуток розраховано **на основі категорій**.")
            elif nan_count > 0:
                print("Імп'ютація: Cost_Per_Unit частково відсутній...")
                good_data = df.dropna(subset=['Cost_Per_Unit', 'Price_Per_Unit'])
                if len(good_data) > 0:
                    avg_margin_ratio = (good_data['Price_Per_Unit'] - good_data['Cost_Per_Unit']).sum() / good_data[
                        'Price_Per_Unit'].sum()
                    if avg_margin_ratio > 0 and avg_margin_ratio < 1:
                        avg_cost_ratio = 1 - avg_margin_ratio
                        df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * avg_cost_ratio, inplace=True)
                        insights.append(
                            f"ℹ️ **Інформація:** Застосовано середню розраховану маржу ({avg_margin_ratio:.1%}) для {nan_count} записів.")
                    else:
                        df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * 0.7, inplace=True)
                else:
                    df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * 0.7, inplace=True)
        else:
            insights.append(f"⚠️ **Увага:** У файлі відсутній стовпець собівартості.")

        if 'Cost_Per_Unit' in df.columns:
            df['Profit'] = df['Revenue'] - (df['Quantity'] * df['Cost_Per_Unit'])
        else:
            df['Profit'] = float('nan')

        df_cleaned = df.dropna(subset=['Revenue'])
        total_revenue = df_cleaned['Revenue'].sum()
        total_transactions = df_cleaned['Transaction_ID'].nunique()
        insights.append(f"✅ Проаналізовано {total_transactions} транзакцій на суму {total_revenue:,.2f} грн.")

        aov = 0
        if total_transactions > 0:
            aov = total_revenue / total_transactions
            insights.append(f"📈 Середній чек (AOV): {aov:,.2f} грн.")

        if 'Product_Category' in df_cleaned.columns:
            df_cleaned['Clean_Category'] = df_cleaned['Product_Category'].apply(get_smart_category)
            category_group = df_cleaned.groupby('Clean_Category')['Revenue'].sum().sort_values(ascending=False)
            if not category_group.empty:
                top_category_name = category_group.idxmax()
                top_category_revenue = category_group.max()
                insights.append(f"🏆 Топ-категорія: '{top_category_name}' ({top_category_revenue:,.2f} грн).")

        if 'Client_Region' in df_cleaned.columns:
            region_group = df_cleaned.groupby('Client_Region')['Revenue'].sum().sort_values(ascending=False)
            if not region_group.empty:
                top_region_name = region_group.idxmax()
                top_region_revenue = region_group.max()
                insights.append(f"🌍 Топ-регіон: '{top_region_name}' ({top_region_revenue:,.2f} грн).")

        if aov > 0:
            target_aov = aov * 1.15
            insights.append(f"💡 **Рекомендація:** Підніміть середній чек до {target_aov:,.2f} грн.")

        return insights

    except Exception as e:
        print(f"Помилка генерації інсайтів: {e}")
        return [f"Не вдалося згенерувати інсайти: {e}"]


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
                print(f"!! Помилка .astype(): {e}")

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
                "message": "Файл успішно завантажено та відправлено в Tableau Cloud!",
                "insights": insights
            }), 200

        except Exception as e:
            return jsonify({"error": f"Помилка обробки файлу: {str(e)}"}), 500
    else:
        return jsonify({"error": "Невірний тип файлу. Потрібен .csv"}), 400


if __name__ == '__main__':
    app.run()