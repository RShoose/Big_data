# **Лекция 2: Практика Hive - Создание ETL-пайплайнов и аналитика**

## **Часть 1: Подготовка и настройка**

### **ETL в распределенных системах: От классики к Big Data**

**Эволюция ETL:**
- **Традиционный ETL:** Extract → Transform → Load (трансформация ДО загрузки)
- **ELT для Big Data:** Extract → Load → Transform (загрузка сырых данных, трансформация ПОСЛЕ)

**Почему ELT доминирует в Hadoop/Hive?**
1. **Стоимость хранения** в HDFS значительно ниже, чем в классических СУБД
2. **Schema-on-Read** позволяет хранить данные без строгой схемы
3. **Вычислительная мощность** MapReduce/Tez/Spark позволяет трансформировать данные на лету

**Трехуровневая архитектура данных в Hive:**
```
Bronze Layer (Raw)      →      Silver Layer (Cleaned)      →      Gold Layer (Aggregated)
      ↓                             ↓                               ↓
• Сырые данные               • Очищенные данные              • Бизнес-метрики
• Внешние таблицы            • Исправленные типы             • Оптимизированные таблицы  
• Минимум трансформаций      • Стандартизированные форматы   • Для отчетности
• Сохранение истории         • Дедупликация                  • Высокая производительность
```
### **Типы таблиц Hive:**

**Внешние таблицы (External Tables):**
```sql
CREATE EXTERNAL TABLE table_name (...) LOCATION '/path/in/hdfs';
```
- **Метаданные отделены от данных** — DROP TABLE не удаляет данные в HDFS
- **Идеально для:** 
  - Данных, управляемых другими системами (Spark, Flink)
  - Многократного использования одних данных разными командами
  - Сценариев "данные как услуга" (data-as-a-service)

**Управляемые таблицы (Managed Tables):**
```sql
CREATE TABLE table_name (...);
```
- **Hive владеет жизненным циклом** — DROP TABLE удаляет и метаданные, и данные
- **Преимущества:**
  - Транзакционная поддержка (ACID) с Hive 3.0
  - Автоматическое управление хранилищем
  - Проще в администрировании

### **Начало работы с Hive**

1. Подключитесь к Hive кластеру:
```bash
docker exec -it hive bash
hive
```

```sql
-- проверяем доступные базы данных
SHOW DATABASES;
SHOW TABLES;
```
### **Партиционирование: Архитектурные паттерны**

**Статическое vs Динамическое партиционирование:**

| **Аспект** | **Статическое** | **Динамическое** |
|------------|-----------------|------------------|
| **Синтаксис** | `PARTITION (dt='2024-01-01')` | `PARTITION (dt)` |
| **Контроль** | Полный | Автоматический |
| **Производительность** | Быстрее для больших вставок | Медленнее (необходима сортировка) |
| **Использование** | Известны все значения партиций | Неизвестны или слишком много значений |

**Динамическое партиционирование в деталях:**
```sql
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Hive автоматически создаст партиции для каждого уникального dt
INSERT INTO table PARTITION(dt) SELECT ..., dt FROM source;
```

**Проблема "большого количества партиций":**
- Каждая партиция → отдельная директория в HDFS
- NameNode хранит метаданные каждой директории в памяти
- **Рекомендация:** < 10K партиций на таблицу

**1. SET hive.exec.dynamic.partition=true;**
**Что происходит:** включает поддержку динамических партиций.

**Без этого параметра:**
```sql
-- придется явно указывать каждую партицию
INSERT OVERWRITE TABLE my_table PARTITION (country='Russia', city='Moscow')
SELECT ...
```

**С этим параметром:**
```sql
-- Hive автоматически создает партиции на основе данных
INSERT OVERWRITE TABLE my_table PARTITION (country, city)
SELECT ..., country, city  -- значения партиций берутся из столбцов
```

**2. SET hive.exec.dynamic.partition.mode=nonstrict;**
**Что происходит:** разрешает полностью динамическое партиционирование.

**Режимы:**
- **strict** (строгий): Требует хотя бы одну статическую партицию
- **nonstrict** (нестрогий): Позволяет все партиции быть динамическими

**Пример разницы:**

```sql
-- в режиме strict - ОШИБКА! Все партиции динамические
INSERT OVERWRITE TABLE movies PARTITION (genre, year)
SELECT title, rating, genre, year FROM source_table;

-- в режиме strict - РАБОТАЕТ. Одна статическая партиция
INSERT OVERWRITE TABLE movies PARTITION (genre='drama', year)
SELECT title, rating, year FROM source_table WHERE genre='drama';
```

**В nonstrict режиме оба запроса работают!**

**3. SET hive.optimize.sort.dynamic.partition=true;**
**Что происходит:** оптимизирует процесс записи в динамические партиции.

**Как работает:**
- Hive сортирует данные по значениям партиций перед записью
- Уменьшается количество открываемых файловых дескрипторов
- Улучшается производительность при большом количестве партиций

**Без оптимизации:**
```
File 1: partition_a/data1
File 2: partition_b/data1  
File 3: partition_a/data2  ← каждый редуктор пишет в разные партиции
File 4: partition_b/data2
```

**С оптимизацией:**
```
File 1: partition_a/data1, partition_a/data2  ← все данные партиции A вместе
File 2: partition_b/data1, partition_b/data2  ← все данные партиции B вместе
```

```sql
-- создаем базу данных для анализа отзывов
CREATE DATABASE IF NOT EXISTS reviews_analysis_text;
USE reviews_analysis_text;

-- настройки Hive
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.optimize.sort.dynamic.partition=true;
```

### **Просмотр данных в HDFS**

```sql
-- проверяем наличие данных
dfs -ls -R /data/lecture_reviews;

-- смотрим содержимое файлов
dfs -cat /data/lecture_reviews/pos/1000023-0.txt | head -3;
```

## **Часть 2: Создание ETL-пайплайна**

### **Создание внешней таблицы (External Table)**

```sql

DROP TABLE IF EXISTS raw_reviews_external;

-- cоздаем внешнюю таблицу с партиционированием
CREATE EXTERNAL TABLE raw_reviews_external (
    review_text STRING
)
PARTITIONED BY (sentiment STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\n'  -- разделитель полей
STORED AS TEXTFILE
LOCATION '/data/lecture_reviews';

-- динамическое добавление партиций
ALTER TABLE raw_reviews_external ADD PARTITION (sentiment='pos')
LOCATION '/data/lecture_reviews/pos';

ALTER TABLE raw_reviews_external ADD PARTITION (sentiment='neg')
LOCATION '/data/lecture_reviews/neg';

ALTER TABLE raw_reviews_external ADD PARTITION (sentiment='neu')
LOCATION '/data/lecture_reviews/neu';

SELECT * FROM raw_reviews_external WHERE sentiment = 'pos' LIMIT 10;

-- или автоматическое добавление всех партиций
MSCK REPAIR TABLE raw_reviews_external;

-- проверяем
SHOW PARTITIONS raw_reviews_external;
SELECT sentiment, COUNT(*) FROM raw_reviews_external GROUP BY sentiment;
```

### **Очистка и трансформация данных**

**ORC (Optimized Row Columnar):**
```
Файл ORC:
├── Stripes (по 64MB)
│   ├── Индекс строки: min, max, count
│   ├── Данные колонок: сжатые
│   └── Footer stripe: статистика
├── File Footer: статистика файла
└── Postscript: компрессия, версия
```

**Преимущества columnar форматов:**
1. **Column pruning:** читаем только нужные колонки
2. **Predicate pushdown:** фильтрация на уровне формата
3. **Векторизация:** пакетная обработка строк
4. **Эффективное сжатие:** одинаковые типы данных → лучшее сжатие

```sql

DROP TABLE IF EXISTS clean_reviews;
-- создаем управляемую таблицу с очищенными данными
CREATE TABLE clean_reviews STORED AS ORC AS
SELECT 
    -- извлекаем ID фильма из имени файла
    regexp_extract(INPUT__FILE__NAME, '/([0-9]+)-[0-9]+\\.txt$', 1) as movie_id,
    
    -- извлекаем ID отзыва
    regexp_extract(INPUT__FILE__NAME, '/[0-9]+-([0-9]+)\\.txt$', 1) as review_id,
    
    -- очищенный текст отзыва
    TRIM(review_text) as review_text,
    
    -- длина отзыва
    LENGTH(TRIM(review_text)) as review_length,
    
    -- исходная тональность
    sentiment,
    
    -- категория длины
    CASE 
        WHEN LENGTH(review_text) > 500 THEN 'very_long'
        WHEN LENGTH(review_text) > 200 THEN 'long'
        WHEN LENGTH(review_text) > 100 THEN 'medium'
        ELSE 'short'
    END as length_category,
    
    -- интенсивность эмоций
    CASE 
        WHEN LOWER(review_text) LIKE '%отличн%' OR LOWER(review_text) LIKE '%превосходн%' 
             OR LOWER(review_text) LIKE '%великолепн%' THEN 'high_positive'
        WHEN LOWER(review_text) LIKE '%хорош%' OR LOWER(review_text) LIKE '%рекоменд%' THEN 'medium_positive'
        WHEN LOWER(review_text) LIKE '%ужасн%' OR LOWER(review_text) LIKE '%кошмар%' THEN 'high_negative'
        WHEN LOWER(review_text) LIKE '%плох%' OR LOWER(review_text) LIKE '%разочарован%' THEN 'medium_negative'
        ELSE 'neutral'
    END as emotion_intensity

FROM raw_reviews_external;

-- проверяем результат
SELECT * FROM clean_reviews LIMIT 5;
SELECT COUNT(*) as total_reviews FROM clean_reviews;
```

### **Создание оптимизированной таблицы с партиционированием и бакетированием**

**Как работает бакетирование:**
```
hash_function(bucketing_column) % num_buckets = bucket_number
```
- Одинаковый ключ → одинаковый бакет
- Равномерное распределение предполагается

**Настройка количества бакетов:**
- **Мало бакетов** (2-10): Крупные файлы, меньше параллелизма
- **Много бакетов** (100+): Мелкие файлы, проблемы с метаданными
- **Золотая середина:** `sqrt(общее_количество_строк)` или эмпирически

**1. `SET hive.enforce.bucketing = true;`**

**Что это:** Принудительное включение бакетирования (разделение на "бакеты").

**Зачем нужно:**
- Без этой настройки Hive может проигнорировать бакетирование
- Гарантирует, что данные будут физически разделены на бакеты


---

**2. `SET hive.exec.dynamic.partition = true;`**

**Что это:** Включает динамическое партиционирование.

**Без динамического партиционирования:**
```sql
-- нужно знать все значения партиций заранее
INSERT OVERWRITE TABLE reviews 
PARTITION (sentiment='pos', year=2023)  -- статические партиции
SELECT ... FROM source WHERE sentiment='pos' AND year=2023;

INSERT OVERWRITE TABLE reviews 
PARTITION (sentiment='neg', year=2023)  -- для каждой комбинации отдельно
SELECT ... FROM source WHERE sentiment='neg' AND year=2023;
```

**С динамическим партиционированием:**
```sql
-- Hive сам определит значения партиций
INSERT OVERWRITE TABLE reviews 
PARTITION (sentiment, year)  -- динамические партиции
SELECT ..., sentiment, year FROM source;
-- Hive автоматически создаст партиции: 
-- /sentiment=pos/year=2023/
-- /sentiment=neg/year=2023/
-- /sentiment=pos/year=2024/ и т.д.
```

---

**3. `SET hive.exec.dynamic.partition.mode = nonstrict;`**

**Что это:** режим динамического партиционирования.

**Два режима:**
1. **`strict` (строгий):**
   - Требует хотя бы одну статическую партицию
   - Защита от случайного создания тысяч партиций

2. **`nonstrict` (нестрогий):**
   - Все партиции могут быть динамическими
   - Полная автоматизация

**Пример разницы:**

```sql
-- В режиме strict - ОШИБКА!
-- Все партиции динамические
INSERT OVERWRITE TABLE reviews 
PARTITION (country, city)  -- обе динамические
SELECT name, country, city FROM users;

-- В режиме strict - РАБОТАЕТ
-- Одна статическая партиция
INSERT OVERWRITE TABLE reviews 
PARTITION (country='Russia', city)  -- country статическая
SELECT name, city FROM users WHERE country='Russia';

-- В режиме nonstrict - РАБОТАЕТ ОБА ВАРИАНТА
```

**Магическое преимущество:**
```sql
-- Если обе таблицы бакетированы одинаково:
SET hive.optimize.bucketmapjoin=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;

-- Join происходит без Shuffle!
SELECT * FROM bucketed_table1 t1 
JOIN bucketed_table2 t2 ON t1.key = t2.key;
```

```sql
-- Включаем необходимые настройки для бакетирования
SET hive.enforce.bucketing = true;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- создаем таблицу с партиционированием по тональности и бакетированием по movie_id
CREATE TABLE optimized_reviews (
    movie_id STRING,
    review_id STRING,
    review_text STRING,
    review_length INT,
    length_category STRING,
    emotion_intensity STRING
)
PARTITIONED BY (sentiment STRING)
CLUSTERED BY (movie_id) INTO 4 BUCKETS  -- БАКЕТИРОВАНИЕ!
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'transactional'='false'
);

-- заполняем таблицу с учетом бакетирования
INSERT OVERWRITE TABLE optimized_reviews PARTITION(sentiment)
SELECT 
    movie_id,
    review_id,
    review_text,
    review_length,
    length_category,
    emotion_intensity,
    sentiment
FROM clean_reviews;

-- проверяем создание
SELECT sentiment, COUNT(*) FROM optimized_reviews GROUP BY sentiment;
DESCRIBE FORMATTED optimized_reviews;
```

## **Часть 3: Аналитические запросы**

### **Базовый анализ**

```sql
-- распределение отзывов по тональности и длине
SELECT 
    sentiment,
    length_category,
    COUNT(*) as review_count,
    ROUND(AVG(review_length), 2) as avg_length,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY sentiment), 2) as percentage_in_group
FROM optimized_reviews
GROUP BY sentiment, length_category
ORDER BY sentiment, review_count DESC;

-- самые обсуждаемые фильмы
SELECT 
    movie_id,
    COUNT(*) as total_reviews,
    SUM(CASE WHEN sentiment = 'pos' THEN 1 ELSE 0 END) as positive,
    SUM(CASE WHEN sentiment = 'neg' THEN 1 ELSE 0 END) as negative,
    SUM(CASE WHEN sentiment = 'neu' THEN 1 ELSE 0 END) as neutral,
    ROUND(AVG(review_length), 2) as avg_length
FROM optimized_reviews
GROUP BY movie_id
HAVING COUNT(*) >= 2
ORDER BY total_reviews DESC
LIMIT 10;
```

### **Анализ с использованием бакетирования**

```sql
-- Пример: запрос с фильтрацией по бакетированному полю (эффективно)
SELECT 
    movie_id,
    sentiment,
    COUNT(*) as review_count
FROM optimized_reviews
WHERE movie_id = '1000023'  -- фильтрация по бакетированному полю
GROUP BY movie_id, sentiment;

-- проанализировать структуру наших данных
SELECT 
    movie_id,
    COUNT(DISTINCT sentiment) as mixed_sentiments,
    COUNT(*) as total_lines,
    COUNT(DISTINCT review_id) as unique_review_files
FROM optimized_reviews
GROUP BY movie_id
ORDER BY mixed_sentiments DESC;

```

### **Семплирование данных (использование бакетирования)**

```sql
-- семплирование на основе бакетов
-- берём данные только из определенных бакетов
SELECT *
FROM optimized_reviews TABLESAMPLE(BUCKET 1 OUT OF 4 ON movie_id)  -- Бакет 1 из 4
WHERE sentiment = 'neg'
LIMIT 5;

-- 1. Быстрая проверка распределения по тональности (10% данных)
-- Показывает: как работает TABLESAMPLE для быстрой статистики
SELECT 
    sentiment,
    COUNT(*) as sample_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM optimized_reviews TABLESAMPLE(10 PERCENT)
GROUP BY sentiment
ORDER BY sample_count DESC;

-- 2. Семплирование конкретного бакета для анализа текста
-- Показывает: как работает TABLESAMPLE с бакетированием + предикаты
SELECT 
    movie_id,
    sentiment,
    SUBSTR(review_text, 1, 50) as text_preview,
    review_length
FROM optimized_reviews TABLESAMPLE(BUCKET 2 OUT OF 4 ON movie_id)
WHERE review_length > 100  -- только содержательные строки
  AND sentiment = 'neg'     -- только негативные
LIMIT 5;

```

## **Часть 4: Сравнение производительности**

```sql
-- Включаем точное измерение времени
SET hive.query.time.seconds.to.sleep.between.jobs=0;

-- 1. ТЕСТ: COUNT с фильтром по sentiment (проверка партиционирования)

-- таблица БЕЗ партиций (clean_reviews)
SELECT 'clean_reviews - COUNT pos' as test, COUNT(*) as result 
FROM clean_reviews 
WHERE sentiment = 'pos';

-- таблица С партициями (optimized_reviews) 
SELECT 'optimized_reviews - COUNT pos' as test, COUNT(*) as result
FROM optimized_reviews 
WHERE sentiment = 'pos';

-- 2. ТЕСТ: Фильтрация по movie_id (проверка бакетирования)

-- clean_reviews (сканирует всё)
SELECT 'clean_reviews - movie_id=1000023' as test, COUNT(*) as result
FROM clean_reviews 
WHERE movie_id = '1000023';

-- optimized_reviews (использует бакеты)
SELECT 'optimized_reviews - movie_id=1000023' as test, COUNT(*) as result
FROM optimized_reviews 
WHERE movie_id = '1000023';

-- 3. ТЕСТ: Комбинированный фильтр (sentiment + movie_id)

-- clean_reviews (полное сканирование + фильтр)
SELECT 'clean_reviews - pos & 1000023' as test, COUNT(*) as result
FROM clean_reviews 
WHERE sentiment = 'pos' AND movie_id = '1000023';

-- optimized_reviews (партиция + бакет)
SELECT 'optimized_reviews - pos & 1000023' as test, COUNT(*) as result
FROM optimized_reviews 
WHERE sentiment = 'pos' AND movie_id = '1000023';

-- 4. ТЕСТ: GROUP BY (агрегация)

-- clean_reviews
SELECT 'clean_reviews - GROUP BY sentiment' as test, COUNT(*) as lines
FROM (
    SELECT sentiment, COUNT(*) as cnt
    FROM clean_reviews
    GROUP BY sentiment
) t;

-- optimized_reviews
SELECT 'optimized_reviews - GROUP BY sentiment' as test, COUNT(*) as lines
FROM (
    SELECT sentiment, COUNT(*) as cnt
    FROM optimized_reviews
    GROUP BY sentiment
) t;

-- 5. ПРОВЕРКА через EXPLAIN (показывает РЕАЛЬНУЮ разницу)

-- посмотрим план для clean_reviews
EXPLAIN
SELECT COUNT(*) 
FROM clean_reviews 
WHERE sentiment = 'pos' AND movie_id = '1000023';

-- посмотрим план для optimized_reviews
EXPLAIN
SELECT COUNT(*)
FROM optimized_reviews
WHERE sentiment = 'pos' AND movie_id = '1000023';

-- 6. ТЕСТ: TABLESAMPLE (показывает преимущество бакетирования)

-- optimized_reviews (читает только 1 бакет = 25% данных)
SELECT 'optimized - TABLESAMPLE bucket 1' as test, COUNT(*) as result
FROM optimized_reviews TABLESAMPLE(BUCKET 1 OUT OF 4 ON movie_id)
WHERE sentiment = 'neg';

-- Для сравнения: если бы делали так в clean_reviews (но нельзя, нет бакетов)
SELECT 'clean - аналогичный объем' as test, COUNT(*) as result
FROM clean_reviews 
WHERE sentiment = 'neg'
LIMIT 50; -- примерно столько строк в одном бакете optimized
```

## **Часть 5: Продвинутые техники**

### **5.1 Индексы и статистика**

```sql
-- сбор статистики для оптимизатора
ANALYZE TABLE optimized_reviews PARTITION(sentiment) COMPUTE STATISTICS;

-- просмотр статистики
DESCRIBE FORMATTED optimized_reviews;
```

### **5.2 Векторизация запросов**

```sql
-- включаем векторизацию для ускорения
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.enabled = true;

-- тестовый запрос с векторизацией
SELECT sentiment, AVG(review_length)
FROM optimized_reviews
GROUP BY sentiment;

-- Hive использует статистику для оценки
EXPLAIN
SELECT sentiment, COUNT(*)
FROM optimized_reviews
WHERE review_length > 200
GROUP BY sentiment;

-- создание VIEW для частых запросов
CREATE VIEW sentiment_stats AS
SELECT 
    sentiment,
    COUNT(*) as total_lines,
    COUNT(DISTINCT movie_id) as unique_movies,
    AVG(review_length) as avg_line_length,
    SUM(CASE WHEN emotion_intensity != 'neutral' THEN 1 ELSE 0 END) as emotional_lines
FROM optimized_reviews
GROUP BY sentiment;

SELECT * FROM sentiment_stats ORDER BY total_lines DESC;

```

## **Быстрые команды для демонстрации**

```sql
-- показ разницы между таблицами
SHOW TABLES;
DESCRIBE FORMATTED optimized_reviews;

-- быстрая проверка бакетирования
SELECT movie_id, INPUT__FILE__NAME 
FROM optimized_reviews 
WHERE sentiment = 'pos' 
LIMIT 5;

-- Проверка размера таблиц
dfs -du -h /user/hive/warehouse/reviews_analysis_text.db/;
```


