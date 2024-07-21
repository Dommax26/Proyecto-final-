import requests
import pandas as pd
import datetime
import psycopg2
from psycopg2 import Error
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configuración del servidor SMTP
SMTP_SERVER = 'smtp.example.com'
SMTP_PORT = 587
SMTP_USER = 'your_email@example.com'
SMTP_PASSWORD = 'your_password'
EMAIL_FROM = 'your_email@example.com'
EMAIL_TO = 'recipient_email@example.com'

def send_error_email(subject, message):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_TO
    msg['Subject'] = subject

    msg.attach(MIMEText(message, 'plain'))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
    except Exception as e:
        print(f"Error al enviar el correo: {e}")

def fetch_exchange_rates(base_url, base_currency):
    url = base_url + base_currency
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception(f"Error al obtener datos de la API: {response.status_code}")

def process_data(data, base_currency, target_currencies):
    base_rate = data["rates"].get(base_currency, 1)
    rates = {currency: data["rates"].get(currency, None) for currency in target_currencies}
    converted_rates = {currency: rate / base_rate for currency, rate in rates.items() if rate is not None}
    return converted_rates

def create_table_if_not_exists(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tipos_cambio (
            fecha DATE,
            moneda_base VARCHAR(3),
            USD DECIMAL(10,4),
            EUR DECIMAL(10,4),
            GBP DECIMAL(10,4),
            JPY DECIMAL(10,4),
            CAD DECIMAL(10,4),
            MXN DECIMAL(10,4),
            AUD DECIMAL(10,4),
            CHF DECIMAL(10,4),
            CNY DECIMAL(10,4),
            SEK DECIMAL(10,4),
            NZD DECIMAL(10,4),
            HKD DECIMAL(10,4),
            NOK DECIMAL(10,4),
            SGD DECIMAL(10,4),
            TRY DECIMAL(10,4),
            ZAR DECIMAL(10,4),
            DKK DECIMAL(10,4),
            RUB DECIMAL(10,4),
            INR DECIMAL(10,4),
            BRL DECIMAL(10,4)
            -- Añade más monedas según sea necesario
        )
        """)
        conn.commit()
    except Error as e:
        error_message = f"Error al crear la tabla: {e}"
        print(error_message)
        send_error_email("Error en la creación de la tabla", error_message)
    finally:
        cursor.close()

def store_data_in_db(df, db_config):
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        create_table_if_not_exists(conn)
        cursor = conn.cursor()

        for index, row in df.iterrows():
            cursor.execute("""
            INSERT INTO tipos_cambio (fecha, moneda_base, USD, EUR, GBP, JPY, CAD, MXN, AUD, CHF, CNY, SEK, NZD, HKD, NOK, SGD, TRY, ZAR, DKK, RUB, INR, BRL)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (row["fecha"], row["moneda_base"], row.get("USD"), row.get("EUR"), row.get("GBP"), row.get("JPY"), row.get("CAD"),
                  row.get("MXN"), row.get("AUD"), row.get("CHF"), row.get("CNY"), row.get("SEK"), row.get("NZD"), row.get("HKD"),
                  row.get("NOK"), row.get("SGD"), row.get("TRY"), row.get("ZAR"), row.get("DKK"), row.get("RUB"), row.get("INR"),
                  row.get("BRL")))

        conn.commit()
    except Error as e:
        error_message = f"Error al conectar o insertar en la base de datos: {e}"
        print(error_message)
        send_error_email("Error en la base de datos", error_message)
    finally:
        if conn:
            cursor.close()
            conn.close()

# Configuración de la base de datos para Amazon Redshift
db_config = {
    "host": "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
    "port": "5439",
    "user": "parela26_coderhouse",
    "password": "FnO723pA7z",
    "dbname": "data-engineer-database"
}

# URL base de la API
base_url = "https://api.exchangerate-api.com/v4/latest/"

# Moneda base
base_currency = "USD"

# Lista de monedas a convertir (actualizada con más monedas)
target_currencies = ["USD", "EUR", "GBP", "JPY", "CAD", "MXN", "AUD", "CHF", "CNY", "SEK", "NZD", "HKD", "NOK", "SGD", "TRY", "ZAR", "DKK", "RUB", "INR", "BRL"]

try:
    data = fetch_exchange_rates(base_url, base_currency)
    converted_rates = process_data(data, base_currency, target_currencies)
    today = datetime.date.today()
    data = {
        "fecha": [today],
        "moneda_base": [base_currency],
        **converted_rates
    }
    df = pd.DataFrame(data)
    print(df.to_string())
    store_data_in_db(df, db_config)

except Exception as e:
    error_message = f"Error en el proceso ETL: {e}"
    print(error_message)
    send_error_email("Error en el proceso ETL", error_message)
