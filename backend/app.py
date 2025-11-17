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
# --- ФІНАЛЬНА ФУНКЦІЯ ІНСАЙТІВ (з Імп'ютацією Собівартості) ---
def generate_insights(df):
    insights = []
    try:
        # --- 1. Підготовка даних (як і раніше) ---
        df['Price_Per_Unit'] = pd.to_numeric(df['Price_Per_Unit'], errors='coerce')
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')

        if 'Cost_Per_Unit' in df.columns:
            df['Cost_Per_Unit'] = pd.to_numeric(df['Cost_Per_Unit'], errors='coerce')

        df['Revenue'] = df['Price_Per_Unit'] * df['Quantity']

        # --- 2. ↓↓↓ НОВА ЛОГІКА: "РОЗУМНА" ІМП'ЮТАЦІЯ СОБІВАРТОСТІ ↓↓↓ ---
        profit_warning = None

        # Перевіряємо, чи є у нас стовпець (ми його створили раніше, але він може бути порожнім)
        if 'Cost_Per_Unit' in df.columns:
            nan_count = df['Cost_Per_Unit'].isnull().sum()
            total_count = len(df)

            if nan_count == total_count:
                # СЦЕНАРІЙ Б: Стовпець повністю порожній. Використовуємо fallback 30% маржі.
                fallback_margin = 0.30  # 30%
                fallback_cost_ratio = 1 - fallback_margin  # 70%

                print("Імп'ютація: Cost_Per_Unit повністю відсутній. Застосовую fallback 70% COGS.")
                df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * fallback_cost_ratio, inplace=True)

                insights.append(
                    f"⚠️ **Увага:** Дані про собівартість (`Cost_Per_Unit`) були відсутні. Для розрахунку прибутку була автоматично застосована **теоретична маржа у 30%**.")

            elif nan_count > 0:
                # СЦЕНАРІЙ A: Стовпець частково порожній. Розраховуємо середню маржу з наявних даних.
                print("Імп'ютація: Cost_Per_Unit частково відсутній. Розраховую середню маржу...")

                # Розраховуємо маржу тільки на "хороших" рядках
                good_data = df.dropna(subset=['Cost_Per_Unit', 'Price_Per_Unit'])
                avg_margin_ratio = (good_data['Price_Per_Unit'] - good_data['Cost_Per_Unit']).sum() / good_data[
                    'Price_Per_Unit'].sum()

                if avg_margin_ratio > 0 and avg_margin_ratio < 1:
                    avg_cost_ratio = 1 - avg_margin_ratio
                    df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * avg_cost_ratio, inplace=True)
                    insights.append(
                        f"ℹ️ **Інформація:** {nan_count} транзакцій не мали собівартості. До них була автоматично застосована **середня розрахована маржа ({avg_margin_ratio:.1%})** з цього файлу.")
                else:
                    # Не змогли розрахувати середню (можливо, Price=0), використовуємо fallback
                    df['Cost_Per_Unit'].fillna(df['Price_Per_Unit'] * (1 - 0.30), inplace=True)
                    insights.append(
                        f"⚠️ **Увага:** Не вдалося розрахувати середню маржу. Для {nan_count} транзакцій була застосована **теоретична маржа у 30%**.")

        # --- 3. ПЕРЕРАХУНОК ПРИБУТКУ ПІСЛЯ ІМП'ЮТАЦІЇ ---
        # Тепер, коли Cost_Per_Unit заповнений, ми можемо розрахувати Прибуток для всіх
        df['Profit'] = df['Revenue'] - (df['Quantity'] * df['Cost_Per_Unit'])

        # --- 4. Продовжуємо Аналіз (з тими даними, що є) ---
        df_cleaned = df.dropna(subset=['Revenue'])
        total_revenue = df_cleaned['Revenue'].sum()
        total_transactions = df_cleaned['Transaction_ID'].nunique()
        insights.append(
            f"✅ Проаналізовано {total_transactions} унікальних транзакцій на загальну суму {total_revenue:,.2f} грн.")

        aov = 0
        if total_transactions > 0:
            aov = total_revenue / total_transactions
            insights.append(f"📈 Середній чек (AOV) у цьому наборі даних становить {aov:,.2f} грн.")

        if 'Product_Category' in df_cleaned.columns:
            category_group = df_cleaned.groupby('Product_Category')['Revenue'].sum().sort_values(ascending=False)
            top_category_name = category_group.idxmax()
            top_category_revenue = category_group.max()
            insights.append(f"🏆 Топ-категорія: '{top_category_name}' з виручкою {top_category_revenue:,.2f} грн.")

        if 'Client_Region' in df_cleaned.columns:
            region_group = df_cleaned.groupby('Client_Region')['Revenue'].sum().sort_values(ascending=False)
            top_region_name = region_group.idxmax()
            top_region_revenue = region_group.max()
            insights.append(f"🌍 Топ-регіон: '{top_region_name}' з виручкою {top_region_revenue:,.2f} грн.")

        # --- 5. Рекомендації (вони спрацюють як і раніше) ---
        if aov > 0:
            target_aov = aov * 1.15
            insights.append(
                f"💡 **Рекомендація:** Ваш середній чек {aov:,.2f} грн. Спробуйте впровадити поріг безкоштовної доставки...")  # (і т.д.)

        if 'Product_Category' in df_cleaned.columns and len(category_group) > 1:
            bottom_category_name = category_group.idxmin()
            bottom_category_revenue = category_group.min()
            insights.append(
                f"📉 **Рекомендація:** Категорія '{bottom_category_name}' приносить найменше доходу ({bottom_category_revenue:,.2f} грн)...")  # (і т.д.)

        return insights

    except Exception as e:
        print(f"Помилка генерації інсайтів: {e}")
        return [f"Не вдалося згенерувати інсайти: {e}"]


# --- Запуск сервера ---
if __name__ == '__main__':
    # Gunicorn буде використовувати цей 'app'
    app.run()