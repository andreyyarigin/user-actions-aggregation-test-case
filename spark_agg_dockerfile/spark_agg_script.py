from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, count, when
import sys
from datetime import datetime, timedelta
import os
import logging
import shutil 


# Получаем дату запуска DAG, передаем через аргументы командной строки
run_date_str = sys.argv[1]
run_date = datetime.strptime(run_date_str, "%Y-%m-%d")

# Задаем папки для входных и временных файлов
input_dir = "/opt/spark/input/"  
spark_temp_dir = '/opt/spark/spark_output_tmp/'  
temp_file_dir = f"{spark_temp_dir}{run_date.strftime('%Y-%m-%d')}" 
output_file_dir = f"/opt/spark/output/{run_date.strftime('%Y-%m-%d')}.csv" 

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(levelname)s: %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S' 
)
logger = logging.getLogger(__name__)  


# Определяем даты за 7 дней до запуска DAG 
start_date = run_date - timedelta(days=7)  # Это начало диапазона
end_date = run_date - timedelta(days=1)  # Это конец диапазона

# Создаем список дат в формате строки для работы с файлами
agg_date_range = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]

logger.info(f"Вычисление агрегата за период с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}")  # для отладки

# Получаем список файлов в папке с входными данными
all_input_files_list = os.listdir(input_dir)
# Отфильтровываем только те файлы, которые являются .csv и начинаются с даты
filtered_files_list = [os.path.join(input_dir, f) for f in all_input_files_list if f.endswith(".csv") and f[:10] in agg_date_range]

# Если отфильтрованные файлы есть, логируем, если нет — выводим ошибку и выходим
if filtered_files_list:
    logger.info(f"Файлы для агрегации: {filtered_files_list}")  # Логируем найденные файлы
else:
    logger.error("Не найдено файлов для агрегации за данный период!")  # Если не нашли
    sys.exit(1)  # Выходим из программы с ошибкой


spark = SparkSession.builder.appName("User Actions Aggregation").getOrCreate()

schema = StructType([
    StructField("email", StringType(), True),  # Поле с email
    StructField("action", StringType(), True),  # Поле с действием пользователя
    StructField("dt", StringType(), True)  # Поле с датой
])

df = spark.read.csv(filtered_files_list, schema=schema, header=False)  # Читаем данные без заголовков

# Группируем данные по email и считаем количество действий пользователей
agg_df = df.groupBy("email").agg(
    count(when(col("action") == "CREATE", True)).alias("create_count"),  # Считаем сколько было CREATE
    count(when(col("action") == "READ", True)).alias("read_count"),  # Считаем сколько было READ
    count(when(col("action") == "UPDATE", True)).alias("update_count"),  # Считаем сколько было UPDATE
    count(when(col("action") == "DELETE", True)).alias("delete_count")  # Считаем сколько было DELETE
)

# Записываем результат работы в временную директорию
agg_df.write.csv(temp_file_dir, mode="overwrite", header=True)
logger.info(f"Результаты работы Spark помещены в {temp_file_dir}")  # Логируем успех записи

# Проверяем, наличие CSV файлов в временной директории
temp_csv_files = [f for f in os.listdir(temp_file_dir) if f.endswith(".csv")]

# Если нет файлов, логируем ошибку и выходим
if not temp_csv_files:
    logger.error("Ошибка при записи данных во временную директорию!") 
    sys.exit(1)  

# CSV файл из временной директории
temp_csv_file = os.path.join(temp_file_dir, temp_csv_files[0])

# Перемещаем в итоговую директорию
shutil.move(temp_csv_file, output_file_dir)
logger.info(f"Агрегированные данные успешно перемещены в файл: {output_file_dir}") 

# Удаляем временную директорию с файлами
shutil.rmtree(temp_file_dir)

spark.stop()
