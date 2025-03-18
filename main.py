from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import clickhouse_connect
from datetime import datetime

# Инициализация FastAPI
app = FastAPI()

# Подключение к ClickHouse
client = clickhouse_connect.get_client(host='127.0.0.1', port=8123, user='default', password='')

# Создание таблицы, если её нет
client.command('''
    CREATE TABLE IF NOT EXISTS test_table (
        name String,
        date DateTime,
        courier_id String
    ) Engine = MergeTree 
    ORDER BY tuple()
''')

# Модель данных для записи
class Record(BaseModel):
    name: str
    courier_id: str

# Добавление записи в ClickHouse
@app.post("/add")
def add_record(record: Record):
    try:
        client.insert('test_table', [(record.name, datetime.now(), record.courier_id)],
                      column_names=['name', 'date', 'courier_id'])
        return {"message": "Record added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Получение всех записей из ClickHouse
@app.get("/records")
def get_records():
    try:
        records = client.query('SELECT * FROM test_table').result_rows
        return {"records": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Удаление записи по courier_id
@app.delete("/delete/{courier_id}")
def delete_record(courier_id: str):
    try:
        client.command(f"ALTER TABLE test_table DELETE WHERE courier_id = '{courier_id}'")
        return {"message": "Record deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))