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
    'Transaction_Date': ['дата', 'дата замовлення', 'date', 'order_date'],
    'Transaction_ID': ['id', 'номер замовлення', 'transaction id', 'order id', 'номер чека'],
    'Product_Category': ['категорія', 'category', 'product category', 'категорія товару'],
    'Quantity': ['кількість', 'quantity', 'qty', 'кіл-ть'],
    'Price_Per_Unit': ['ціна', 'price', 'ціна за од'],
    'Cost_Per_Unit': ['собівартість', 'cost', 'cost per unit'],
    'Client_Region': ['регіон', 'місто', 'region', 'city', 'client region', 'регіон доставки'],
}

# --- СХЕМА ДАНИХ (БЕЗ Profit/Revenue) ---
TABLEAU_SCHEMA = {
    'Transaction_Date': 'datetime64[ns]',
    'Transaction_ID': 'object',
    'Product_Category': 'object',
    'Quantity': 'Int64',
    'Price_Per_Unit': 'float64',
    'Cost_Per_Unit': 'float64',
    'Client_Region': 'object'
}

# --- "РОЗУМНИЙ" СЛОВНИК МАРЖІ (Без змін) ---
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

# --- "РОЗУМНА" ФУНКЦІЯ ДЛЯ КАТЕГОРІЙ (Без змін) ---
CLEAN_CATEGORIES = list(MARGIN_FALLBACKS_BY_CATEGORY.keys())


def get_smart_category(dirty_category):
    if not isinstance(dirty_category, str):
        return 'default'
    best_match, score = process.extractOne(dirty_category.lower(), CLEAN_CATEGORIES, scorer=fuzz.token_set_ratio)
    if score > 60:
        return best_match
    else:
        return 'default'


# --- Функція Публікації (Без змін) ---
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
            # --- ↓↓↓ ПРИМУСОВЕ ОНОВЛЕННЯ (REFRESH) ↓↓↓ ---
            print("Запускаю примусове оновлення (refresh) джерела...")
            try:
                server.datasources.refresh(datasource_to_update)
                print("Примусове оновлення (refresh) успішно запущено.")
            except Exception as e:
                print(f"!! Помилка під час запуску 'refresh', але це не критично: {e}")
            return None
    except TSC.ServerResponseError as e:
        error_msg = f"Помилка Tableau API: {e.summary} - {e.detail}"
        print(f"!! {error_msg}")
        return error_msg
    except Exception as e:
        error_msg = f"Критична помилка Python: {str(e)}"
        print(f"!! {error_msg}")
        return error_msg


# --- Функція Мапінгу (Без змін) ---
def smart_column_mapping(uploaded_columns):
    mapping = {}
    all_standard_options = []
    for standard_name, variations in STANDARD_COLUMNS.items():
        for var in variations:
            all_standard_options.append((var, standard_name))
    choices_dict = {opt[0]: opt[1] for opt in all_standard_options}
    choice_keys = list(choices_dict.keys())
    print(f"Вхідні стовпці: {uploaded_columns}")
    for col in uploaded_columns:
        clean_col = col.lower().strip().replace('_', ' ')
        best_match, score = process.extractOne(clean_col, choice_keys, scorer=fuzz.token_sort_ratio)
        if score > 60:
            standard_name = choices_dict[best_match]
            mapping[col] = standard_name
            print(f"Знайдено: '{col}' -> '{standard_name}' (Схожість: {score}%)")
        else:
            print(f"НЕ знайдено: '{col}' (Найкращий варіант: '{best_match}' з {score}%)")
    return mapping


# --- ФУНКЦІЯ ІНСАЙТІВ (Яка НЕ розраховує Profit/Revenue самостійно) ---
def generate_insights(df):
    insights = []
    try:
        # --- 1. Підготовка даних ---
        df['Price_Per_Unit'] = pd.to_numeric(df['Price_Per_Unit'], errors='coerce')
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
        if 'Cost_Per_Unit' in df.columns:
            df['Cost_Per_Unit'] = pd.to_numeric(df['Cost_Per_Unit'], errors='coerce')

        # --- 2. РОЗУМНА ІМП'ЮТАЦІЯ СОБІВАРТОСТІ ---
        # (Цей блок потрібен, щоб ЗАПОВНИТИ Cost_Per_Unit, якщо він порожній)
        if 'Cost_Per_Unit' in df.columns:
            nan_count = df['Cost_Per_Unit'].isnull().sum()
            total_count = len(df)
            if nan_count == total_count:
                if 'Product_Category' not in df.columns:
                    fallback_margin = MARGIN_FALLBACKS_BY_CATEGORY['default']
                    df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * (1 - fallback_margin), inplace=True)
                    insights.append(
                        f"⚠️ **Увага:** Собівартість (`Cost_Per_Unit`) ТА Категорії (`Product_Category`) відсутні. Для розрахунку прибутку була застосована **загальна теоретична маржа у {fallback_margin:.0%}**.")
                else:
                    print("Імп'ютація: Cost_Per_Unit повністю відсутній. Застосовую 'розумну' маржу за категоріями...")
                    df['Cost_Per_Unit'] = df.apply(
                        lambda row: row['Price_Per_Unit'] * (
                                    1 - MARGIN_FALLBACKS_BY_CATEGORY[get_smart_category(row['Product_Category'])]),
                        axis=1
                    )
                    insights.append(
                        f"⚠️ **Увага:** Собівартість (`Cost_Per_Unit`) була відсутня. Прибуток розраховано **на основі 'розумного' зіставлення категорій** (напр., 'gadgets' -> 'Electronics').")
            elif nan_count > 0:
                print("Імп'ютація: Cost_Per_Unit частково відсутній. Розраховую середню маржу...")
                good_data = df.dropna(subset=['Cost_Per_Unit', 'Price_Per_Unit'])
                avg_margin_ratio = (good_data['Price_Per_Unit'] - good_data['Cost_Per_Unit']).sum() / good_data[
                    'Price_Per_Unit'].sum()
                if avg_margin_ratio > 0 and avg_margin_ratio < 1:
                    avg_cost_ratio = 1 - avg_margin_ratio
                    df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * avg_cost_ratio, inplace=True)
                    insights.append(
                        f"ℹ️ **Інформація:** {nan_count} транзакцій не мали собівартості. До них була автоматично застосована **середня розрахована маржа ({avg_margin_ratio:.1%})** з цього файлу.")
                else:
                    df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * (1 - 0.30), inplace=True)
                    insights.append(
                        f"⚠️ **Увага:** Не вдалося розрахувати середню маржу. Для {nan_count} транзакцій була застосована **теоретична маржа у 30%**.")

        # --- 3. Продовжуємо Аналіз (БЕЗ розрахунку Profit/Revenue) ---
        # (Ми розрахуємо їх у Tableau)

        # Розрахуємо Revenue тимчасово ЛИШЕ для інсайтів
        df_temp_revenue = (df['Price_Per_Unit'] * df['Quantity'])
        total_revenue = df_temp_revenue.sum()
        total_transactions = df['Transaction_ID'].nunique()
        insights.append(
            f"✅ Проаналізовано {total_transactions} унікальних транзакцій на загальну суму {total_revenue:,.2f} грн.")

        aov = 0
        if total_transactions > 0:
            aov = total_revenue / total_transactions
            insights.append(f"📈 Середній чек (AOV) у цьому наборі даних становить {aov:,.2f} грн.")

        if 'Product_Category' in df.columns:
            df['Temp_Revenue'] = df_temp_revenue  # Додаємо тимчасову виручку
            df['Clean_Category'] = df['Product_Category'].apply(get_smart_category)
            category_group = df.groupby('Clean_Category')['Temp_Revenue'].sum().sort_values(ascending=False)
            top_category_name = category_group.idxmax()
            top_category_revenue = category_group.max()
            insights.append(f"🏆 Топ-категорія: '{top_category_name}' з виручкою {top_category_revenue:,.2f} грн.")

        if 'Client_Region' in df.columns:
            region_group = df.groupby('Client_Region')['Temp_Revenue'].sum().sort_values(ascending=False)
            top_region_name = region_group.idxmax()
            top_region_revenue = region_group.max()
            insights.append(f"🌍 Топ-регіон: '{top_region_name}' з виручкою {top_region_revenue:,.2f} грн.")

        if aov > 0:
            target_aov = aov * 1.15
            insights.append(
                f"💡 **Рекомендація:** Ваш середній чек {aov:,.2f} грн. Спробуйте впровадити поріг безкоштовної доставки...")

        if 'Product_Category' in df.columns and len(category_group) > 1:
            bottom_category_name = category_group.idxmin()
            bottom_category_revenue = category_group.min()
            insights.append(
                f"📉 **Рекомендація:** Категорія '{bottom_category_name}' приносить найменше доходу ({bottom_category_revenue:,.2f} грн)...")

        return insights

    except Exception as e:
        print(f"Помилка генерації інсайтів: {e}")
        return [f"Не вдалося згенерувати інсайти: {e}"]


# --- ГОЛОВНИЙ API ENDPOINT (ОСТАТОЧНА ВЕРСІЯ) ---
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

            # 1. Гарантуємо, що ВСІ стовпці існують
            all_standard_keys = list(STANDARD_COLUMNS.keys())
            df_standard = pd.DataFrame(columns=all_standard_keys)
            final_columns = [col for col in all_standard_keys if col in df.columns]
            df_final = pd.concat([df_standard, df[final_columns]], sort=False)

            # 2. ПРИМУСОВЕ ЗАСТОСУВАННЯ ТИПІВ ДАНИХ (Виправляє "Arrow type: na")
            print("Примусово застосовую типи даних...")
            try:
                if 'Transaction_Date' in df_final.columns:
                    df_final['Transaction_Date'] = pd.to_datetime(df_final['Transaction_Date'], errors='coerce')
                df_final = df_final.astype(TABLEAU_SCHEMA, errors='ignore')
            except Exception as e:
                print(f"!! Помилка при застосуванні схеми .astype(): {e}")

            # 3. Генеруємо інсайти ТА ЗАПОВНЮЄМО 'Cost_Per_Unit'
            insights = generate_insights(df_final)

            # 4. Зберігаємо файл ТИМЧАСОВО у .hyper форматі
            temp_file_path = os.path.join('temp_cleaned_data.hyper')
            print(f"Конвертую дані у {temp_file_path}...")

            # ВАЖЛИВО: Ми передаємо df_final (який тепер має ЗАПОВНЕНИЙ Cost_Per_Unit)
            pt.frame_to_hyper(df_final, temp_file_path, table='Extract')

            # 5. Викликаємо нашу функцію для завантаження в хмару
            print("Запускаю оновлення даних в Tableau Cloud...")
            tableau_error = publish_to_tableau_cloud(temp_file_path)

            # 6. Видаляємо тимчасовий файл
            os.remove(temp_file_path)

            # 7. Перевіряємо, чи є помилка
            if tableau_error:
                insights.append(f"ПОМИЛКА TABLEAU: {tableau_error}")

            # 8. Повертаємо інсайти
            return jsonify({
                "message": "Файл успішно завантажено та відправлено в Tableau Cloud!",
                "insights": insights
            }), 200

        except Exception as e:
            return jsonify({"error": f"Помилка обробки файлу: {str(e)}"}), 500
    else:
        return jsonify({"error": "Невірний тип файлу. Потрібен .csv"}), 400


# --- Запуск сервера ---
if __name__ == '__main__':
    app.run()