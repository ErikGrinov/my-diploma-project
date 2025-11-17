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
# Цей рядок правильний, він жорстко прописує твій Vercel URL
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


# --- Функція Публікації ---
def publish_to_tableau_cloud(file_path):
    """
    Підключається до Tableau Cloud і повертає None у разі успіху,
    або рядок з помилкою у разі невдачі.
    """
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
            return None  # <-- Успіх! Повертаємо None

    except TSC.ServerResponseError as e:
        # Ловимо специфічну помилку Tableau
        error_msg = f"Помилка Tableau API: {e.summary} - {e.detail}"
        print(f"!! {error_msg}")
        return error_msg
    except Exception as e:
        # Ловимо всі інші помилки
        error_msg = f"Критична помилка Python: {str(e)}"
        print(f"!! {error_msg}")
        return error_msg


# --- Функція Мапінгу ---
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


# --- Функція Інсайтів ---
def generate_insights(df):
    insights = []
    try:
        # --- Підготовка даних ---
        df['Price_Per_Unit'] = pd.to_numeric(df['Price_Per_Unit'], errors='coerce')
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
        df.dropna(subset=['Price_Per_Unit', 'Quantity'], inplace=True)

        df['Revenue'] = df['Price_Per_Unit'] * df['Quantity']

        total_revenue = df['Revenue'].sum()
        total_transactions = df['Transaction_ID'].nunique()

        # --- ОПИСОВІ ІНСАЙТИ ---
        insights.append(
            f"✅ Проаналізовано {total_transactions} унікальних транзакцій на загальну суму {total_revenue:,.2f} грн.")

        aov = 0
        if total_transactions > 0:
            aov = total_revenue / total_transactions
            insights.append(f"📈 Середній чек (AOV) у цьому наборі даних становить {aov:,.2f} грн.")

        if 'Product_Category' in df.columns:
            category_group = df.groupby('Product_Category')['Revenue'].sum().sort_values(ascending=False)
            top_category_name = category_group.idxmax()
            top_category_revenue = category_group.max()
            insights.append(f"🏆 Топ-категорія: '{top_category_name}' з виручкою {top_category_revenue:,.2f} грн.")

        if 'Client_Region' in df.columns:
            region_group = df.groupby('Client_Region')['Revenue'].sum().sort_values(ascending=False)
            top_region_name = region_group.idxmax()
            top_region_revenue = region_group.max()
            insights.append(f"🌍 Топ-регіон: '{top_region_name}' з виручкою {top_region_revenue:,.2f} грн.")

        # --- 💡 ПРИПИСОВІ РЕКОМЕНДАЦІЇ ---
        if aov > 0:
            target_aov = aov * 1.15
            insights.append(
                f"💡 **Рекомендація:** Ваш середній чек {aov:,.2f} грн. Спробуйте впровадити поріг безкоштовної доставки (наприклад, від {target_aov:,.2f} грн) або додайте 'cross-sell' товари, щоб заохотити клієнтів купувати більше.")

        if 'Product_Category' in df.columns and len(category_group) > 1:
            bottom_category_name = category_group.idxmin()
            bottom_category_revenue = category_group.min()
            insights.append(
                f"📉 **Рекомендація:** Категорія '{bottom_category_name}' приносить найменше доходу ({bottom_category_revenue:,.2f} грн). Розгляньте можливість проведення цільової промо-акції для неї або проаналізуйте її асортимент, щоб підвищити привабливість.")

        return insights

    except Exception as e:
        print(f"Помилка генерації інсайтів: {e}")
        return ["Не вдалося автоматично згенерувати інсайти для цього файлу."]


# --- ГОЛОВНИЙ API ENDPOINT ---
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

            final_columns = [col for col in STANDARD_COLUMNS.keys() if col in df.columns]
            df_final = df[final_columns].copy()

            if 'Price_Per_Unit' in df_final.columns and 'Quantity' in df_final.columns:
                insights = generate_insights(df_final)
            else:
                insights = ["Аналіз неможливий: відсутні стовпці 'Price_Per_Unit' або 'Quantity'."]

            # 1. Зберігаємо файл ТИМЧАСОВО у .hyper форматі
            temp_file_path = os.path.join('temp_cleaned_data.hyper')  # <-- Змінили .csv на .hyper
            print(f"Конвертую дані у {temp_file_path}...")

            # 'Extract' - це стандартна назва таблиці, яку Tableau очікує.
            pt.frame_to_hyper(df_final, temp_file_path, table='Extract')  # <-- Замінили .to_csv на pantab

            # 2. Викликаємо нашу функцію для завантаження в хмару
            print("Запускаю оновлення даних в Tableau Cloud...")
            # Функція тепер повертає None або рядок з помилкою
            tableau_error = publish_to_tableau_cloud(temp_file_path)

            # 3. Видаляємо тимчасовий файл
            os.remove(temp_file_path)

            # 4. Перевіряємо, чи є помилка
            if tableau_error:
                # Якщо функція повернула помилку, додаємо її до "інсайтів"
                insights.append(f"ПОМИЛКА TABLEAU: {tableau_error}")

            # 5. Повертаємо інсайти
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
    # Gunicorn буде використовувати цей 'app'
    app.run()