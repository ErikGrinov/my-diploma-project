import os
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from fuzzywuzzy import fuzz, process

# --- Налаштування Сервера ---
app = Flask(__name__)
# Дозволяємо запити з localhost:3000 (де буде React)
CORS(app, resources={r"/api/*": {"origins": "https://my-diploma-project.vercel.app"}})

# --- Шлях до нашого головного файлу ---
# Цей файл Tableau буде читати (або ви будете оновлювати)
DATA_FILE_PATH = os.path.join('data', 'standard_sales_data.csv')

# --- НАША СТАНДАРТНА МОДЕЛЬ ДАНИХ ---
# Це стовпці, які очікує дашборд в Tableau
STANDARD_COLUMNS = {
    'Transaction_Date': ['дата', 'дата замовлення', 'date', 'order_date'],

    # Додано 'номер чека', щоб розпізнавати 'Номер_Чека'
    'Transaction_ID': ['id', 'номер замовлення', 'transaction id', 'order id', 'номер чека'],

    # Додано 'категорія товару'
    'Product_Category': ['категорія', 'category', 'product category', 'категорія товару'],

    # Додано 'кіл-ть'
    'Quantity': ['кількість', 'quantity', 'qty', 'кіл-ть'],

    # Додано 'ціна' (про всяк випадок, хоча вона вже була)
    'Price_Per_Unit': ['ціна', 'price', 'ціна за од'],

    'Cost_Per_Unit': ['собівартість', 'cost', 'cost per unit'],

    # Додано 'регіон доставки'
    'Client_Region': ['регіон', 'місто', 'region', 'city', 'client region', 'регіон доставки'],
}


def smart_column_mapping(uploaded_columns):
    """
    "Розумна" функція, яка знаходить найкращу відповідність
    для кожного стовпця зі STANDARD_COLUMNS.
    """
    mapping = {}
    # Робимо список всіх можливих назв з STANDARD_COLUMNS
    all_standard_options = []
    for standard_name, variations in STANDARD_COLUMNS.items():
        for var in variations:
            # Зберігаємо пару (варіант, стандартна назва)
            all_standard_options.append((var, standard_name))

    # Створюємо словник {варіант: стандартна_назва}
    # наприклад: {'дата': 'Transaction_Date', 'date': 'Transaction_Date', ...}
    choices_dict = {opt[0]: opt[1] for opt in all_standard_options}

    # Список тільки варіантів назв для fuzzywuzzy
    choice_keys = list(choices_dict.keys())

    print(f"Вхідні стовпці: {uploaded_columns}")

    for col in uploaded_columns:
        # Приводимо до нижнього регістру та прибираємо пробіли/підкреслення
        clean_col = col.lower().strip().replace('_', ' ')

        # Знаходимо найкраще співпадіння з нашого списку
        # process.extractOne повертає (найкращий_варіант, % схожості)
        best_match, score = process.extractOne(clean_col, choice_keys, scorer=fuzz.token_sort_ratio)

        # Встановлюємо поріг схожості (наприклад, 60%)
        if score > 60:
            # Знаходимо стандартну назву, якій відповідає цей варіант
            standard_name = choices_dict[best_match]

            # Додаємо у мапінг {Вхідна_назва: Стандартна_назва}
            mapping[col] = standard_name
            print(f"Знайдено: '{col}' -> '{standard_name}' (Схожість: {score}%)")
        else:
            print(f"НЕ знайдено: '{col}' (Найкращий варіант: '{best_match}' з {score}%)")

    return mapping


def generate_insights(df):
    """
    Аналізує очищений DataFrame і генерує список текстових інсайтів
    та ПРИПИСОВИХ РЕКОМЕНДАЦІЙ.
    """
    insights = []
    try:
        # --- Підготовка даних ---
        df['Price_Per_Unit'] = pd.to_numeric(df['Price_Per_Unit'], errors='coerce')
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
        df.dropna(subset=['Price_Per_Unit', 'Quantity'], inplace=True)  # Видаляємо рядки, де ціни/кількості немає

        df['Revenue'] = df['Price_Per_Unit'] * df['Quantity']

        total_revenue = df['Revenue'].sum()
        total_transactions = df['Transaction_ID'].nunique()

        # --- ОПИСОВІ ІНСАЙТИ (Що сталося?) ---
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

        # --- 💡 ПРИПИСОВІ РЕКОМЕНДАЦІЇ (Що робити?) ---

        # 1. Рекомендація на основі AOV (Середній чек)
        if aov > 0:
            # Пропонуємо підняти середній чек на 15%
            target_aov = aov * 1.15
            insights.append(
                f"💡 **Рекомендація:** Ваш середній чек {aov:,.2f} грн. Спробуйте впровадити поріг безкоштовної доставки (наприклад, від {target_aov:,.2f} грн) або додайте 'cross-sell' товари, щоб заохотити клієнтів купувати більше.")

        # 2. Рекомендація на основі найгіршої категорії
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
            # 1. Читаємо завантажений CSV
            df = pd.read_csv(file)

            # 2. Отримуємо "розумний" мапінг
            column_mapping = smart_column_mapping(df.columns.tolist())

            # 3. Перейменовуємо стовпці
            df.rename(columns=column_mapping, inplace=True)

            # 4. Залишаємо тільки ті стовпці, які нам потрібні
            final_columns = [col for col in STANDARD_COLUMNS.keys() if col in df.columns]
            df_final = df[final_columns].copy()  # Використовуємо .copy(), щоб уникнути попереджень

            # 5. ГЕНЕРУЄМО "РОЗУМНІ РЕКОМЕНДАЦІЇ"
            # Перевіряємо, чи є в нас мінімально необхідні дані для аналізу
            if 'Price_Per_Unit' in df_final.columns and 'Quantity' in df_final.columns:
                insights = generate_insights(df_final)
            else:
                insights = ["Аналіз неможливий: відсутні стовпці 'Price_Per_Unit' або 'Quantity'."]

            # 6. ЗБЕРІГАЄМО ФАЙЛ
            df_final.to_csv(DATA_FILE_PATH, index=False)

            # 7. Повертаємо інсайти разом з відповіддю
            return jsonify({
                "message": "Файл успішно завантажено та оброблено!",
                "mapped_columns": column_mapping,
                "final_columns": final_columns,
                "insights": insights  # <-- НАШІ НОВІ ІНСАЙТИ
            }), 200

        except Exception as e:
            return jsonify({"error": f"Помилка обробки файлу: {str(e)}"}), 500
    else:
        return jsonify({"error": "Невірний тип файлу. Потрібен .csv"}), 400


# --- Запуск сервера ---
if __name__ == '__main__':
    # Створюємо папку data, якщо її немає
    if not os.path.exists('data'):
        os.makedirs('data')

    app.run(debug=True, port=5000)  # Сервер буде працювати на http://localhost:5000