## **Вебинар 7: Сложные паттерны MapReduce в ритейл-аналитике**

### **Теоретическая часть** 

---

## **1. Вторичная сортировка (Secondary Sort) в ритейле** 

### **Проблема: Анализ временных рядов**
```python
# Без вторичной сортировки - данные вразброс
("2023-01", "Books")    → 4500
("2023-02", "Electronics") → 12000  
("2023-01", "Electronics") → 15000
("2023-02", "Books")    → 5200
```

### **Составной ключ с группировкой**
```python
# С вторичной сортировкой - упорядоченные данные
("2023-01", "Books")       → 4500
("2023-01", "Clothing")    → 8900
("2023-01", "Electronics") → 15000
("2023-02", "Books")       → 5200
("2023-02", "Clothing")    → 10200
("2023-02", "Electronics") → 12000
```

### **Архитектура Secondary Sort**
```
📊 Данные → 🗺️ Mapper → 🔄 Partitioner → 📂 Group Comparator → ♻️ Reducer
    ↓           ↓              ↓                 ↓                 ↓
   Raw        (K, V)      По году-месяцу    По категории      Агрегация
```

### **Продажи по месяцам и категориям**
```
 Выручка (тыс.$)
    │
20  │    ████████████████
    │    ████████████████ Electronics
15  │    ██████████      ██████████
    │    ██████████      ██████████ Clothing  
10  │    ████            ██████    ██████
    │    ████            ██████    ██████ Books
5   │    ██              ██        ██
    └─────────────────────────────────────
       Янв    Фев    Мар    Апр    Май
```

---

## **2. Составные ключи для многомерного анализа** 

### **Проблема: Множественные срезы данных**
Традиционный подход требует нескольких проходов:
1. 🗺️ Анализ по полу
2. 🗺️ Анализ по категориям  
3. 🗺️ Анализ по возрастным группам
4. 🗺️ Анализ по регионам

### **Единый проход с составными ключами**
```python
# Многомерная агрегация в одном Job
("Gender-Male", "Revenue")        → 150000
("Gender-Female", "Revenue")      → 120000
("Category-Electronics", "Revenue") → 90000
("Age-25-34", "Revenue")          → 80000
("Gender-Category-Male-Electronics", "Revenue") → 45000
```

### **Матрица потребительского поведения**
```
         Electronics  Clothing  Books  Home
Male       45,000     35,000   15,000 25,000
Female     30,000     55,000   20,000 15,000

        18-24      25-34      35-44      45+
Male   20,000     45,000     35,000   20,000  
Female 25,000     40,000     35,000   20,000
```

### **Гендерное распределение по категориям**
```
 Распределение покупок по полу и категориям
100% │
     │    ██████████████    ██████████████
     │    ██████████████    ██████████████
 75% │    ██████████████    ██████░░░░░░░░
     │    ██████████████    ██████░░░░░░░░ Female
 50% │    ██████░░░░░░░░    ██████░░░░░░░░
     │    ██████░░░░░░░░    ██████░░░░░░░░ Male
 25% │    ██████░░░░░░░░    ██████░░░░░░░░
     │    ██████░░░░░░░░    ██████░░░░░░░░
  0% └─────────────────────────────────────
      Electronics   Clothing   Books    Home
```

---

## **3. Multiple Outputs - единый проход для всей аналитики** 

### **Архитектура комплексного анализа**
```
📈 Входные данные
    ↓
🔄 Единый Mapper
    ↓  
📊 Multiple Outputs
    ├── 🗓️  Продажи по месяцам
    ├── 👥  Анализ по полу
    ├── 📦  Анализ по категориям
    ├── 🎯  Составные метрики
    └── 📈  Общая статистика
```

### **Ключевые метрики ритейл-аналитики**
```python
# В одном Mapper-е собираем все метрики
METRICS = {
    "Временные ряды": ["MONTHLY", "WEEKLY", "DAILY"],
    "Продуктовые": ["CATEGORY", "SUBCATEGORY", "BRAND"], 
    "Клиентские": ["GENDER", "AGE_GROUP", "REGION"],
    "Транзакционные": ["AVG_RECEIPT", "BASKET_SIZE", "FREQUENCY"],
    "Составные": ["GENDER_CATEGORY", "AGE_REGION", "SEASONALITY"]
}
```

### **Комплексная дашборд-аналитика**
```
RETAIL ANALYTICS DASHBOARD
┌─────────────────┬─────────────────┬─────────────────┐
│ВРЕМЕННЫЕ РЯДЫ   │    КЛИЕНТЫ      │   ПРОДУКТЫ      │
├─────────────────┼─────────────────┼─────────────────┤
│  150K ↗︎ 15%     │   Male: 55%     │  Electronics    │
│  125K ████████  │   Female: 45%   │  Clothing ████  │
│  100K ██████    │                 │  Books    ██    │
│   75K ████      │   25-34: 40%    │  Home     ██    │
│   50K ██        │   35-44: 30%    │                 │
│   25K █         │   18-24: 20%    │  Avg Receipt:   │
│    0K           │   45+: 10%      │    $85.20 ↗︎     │
│   J F M A M J   │                 │                 │
└─────────────────┴─────────────────┴─────────────────┘
```

---

## **4. Бизнес-кейсы применения** (10 мин)

### **Кейс 1: Сезонность продаж**
```
🎄 СЕЗОННОСТЬ ПРОДАЖ ПО КАТЕГОРИЯМ
    │
200%│              ██████████
    │              ██████████ Electronics 
150%│    ██████    ██    ██  ██████
    │    ██████    ██    ██  ██████ Clothing
100%│    ██  ██    ██    ██  ██  ██
    │    ██  ██    ██    ██  ██  ██ Books
 50%│  ██    ██████    ██    ██    ██
    │  ██    ██████    ██    ██    ██
    └────────────────────────────────────
       Q1   Q2   Q3   Q4   Holidays
```

### **Кейс 2: Сегментация покупателей**
```
СЕГМЕНТАЦИЯ ПОКУПАТЕЛЕЙ
High-Value    ██████████ 18%  | $500+ avg
Medium-Value  ██████████████ 25%  | $200-500  
Low-Value     ██████████████████ 35% | $50-200
Occasional    ███████████ 22%  | <$50

ВОЗРАСТНЫЕ СЕГМЕНТЫ
18-24: ██████ 20%   | Tech-savvy, impulse buys
25-34: ████████████ 35%   | Family needs, value
35-44: ██████████ 28%   | Quality focused  
45+:   █████ 17%   | Brand loyal, traditional
```

### **Кейс 3: Эффективность маркетинга**
```
ROI ПО КАНАЛАМ ПРОДАЖ
Online   ██████████████ 45%  | $2.10 ROI
Mobile   ██████████ 32%  | $1.80 ROI
Store    ██████ 23%  | $1.20 ROI

 ЦЕЛЕВЫЕ АУДИТОРИИ
Electronics: Male 25-34 ██████████
Clothing: Female 18-29 ████████████  
Books: Mixed 25-45 ████████
Home: Female 35-55 ██████
```

---

## **5. Техническая архитектура** (5 мин)

### **Оптимизация производительности**
```
⚡ ПАТТЕРНЫ ОПТИМИЗАЦИИ

Комбайнеры:    Локальная агрегация на mapper-узлах
Партиционирование: Равномерное распределение нагрузки  
In-Mapper Combiner: Уменьшение network I/O
Multiple Outputs: Избежание повторных проходов

МЕТРИКИ ЭФФЕКТИВНОСТИ

Data Locality:    95%  ✅
Network Transfer: 2.1GB  ✅  
Execution Time:   3.2min ✅
Resource Usage:   78%   ✅
```
---

## **Ограничения MapReduce - Когда не стоит использовать**

### **Проблемы архитектуры**
```
MapReduce ПРОБЛЕМЫ:

• Итеративные алгоритмы - Многократные проходы по данным
• Интерактивные запросы - Высокая latency (>минуты)
• Сложные зависимости - Цепочки MapReduce Jobs
• Join операций - Ресурсоемкие Shuffle-стадии
• Small Files - Проблемы производительности
• Сложность отладки - Распределенная система
```

### **Сравнение подходов**
```
ПЛАТФОРМА          ЛАТЕНТНОСТЬ    ТИП НАГРУЗКИ
MapReduce          Минуты+        Пакетная обработка
Apache Spark       Секунды-минуты Пакетная+микро-пакеты
Apache Flink       Миллисекунды   Реальная время
Базы данных        Миллисекунды   Интерактивная
```

---

## **Проблема итеративных вычислений**

### **Пример: Машинное обучение**
```
ITERATIVE PROCESSING PROBLEM

Data → Map → Reduce → Write → Read → Map → Reduce...
    ↓
На каждом шаге:                     Альтернатива:
• Чтение с диска                    • Кэширование в памяти  
• Запись на диск                    • Итерации в памяти
• Высокие задержки                  • Низкая latency
```

### **Визуализация: MapReduce vs Spark**
```
ВРЕМЯ ВЫПОЛНЕНИЯ (10 итераций)
MapReduce: ██████████ 100сек
           ↑ Диск ↑ Диск ↑ Диск
Spark:     ███ 30сек
           ↑ Память ↑ Память
```

---

## **Проблема интерактивных запросов**

### **Latency сравнение**
```
⏱️ ВРЕМЯ ОТВЕТА СИСТЕМ

MapReduce:   "Приходи завтра за результатами"
             ↓
             Job Setup (10-30сек) → 
             Data Read → 
             Map Phase → 
             Shuffle → 
             Reduce Phase → 
             Output (2-10 минут)

Apache Spark: "Результат через секунды"
             ↓  
             In-Memory (1-30 секунд)

Базы данных:  "Мгновенный ответ"  
             ↓
             Index Lookup (1-1000мс)
```

### **Use-case сравнение**
```
ЗАПРОС: "Топ-10 товаров за сегодня"

MapReduce:  Не подходит - очень долго
Spark SQL:  Идеально - 5-10 секунд  
ClickHouse: Идеально - 100-500мс
HBase:      Хорошо - 1-3 секунды
```

---

## **Сложность программирования**

### **Сравнение кода**
```python
# MapReduce (50+ строк)
class SalesMapper:
    def map(self):
        # Ручная обработка
        # Ручная сериализация
        # Ручная агрегация

class SalesReducer:
    def reduce(self):
        # Ручная группировка
        # Ручная запись

# Spark (10 строк)
df = spark.read.parquet("sales")
result = (df
    .groupBy("category")
    .agg(sum("revenue"))
    .orderBy("revenue")
)
```

### **Проблемы разработки**
```
СЛОЖНОСТИ ОТЛАДКИ:

• Логи распределены по кластеру
• Нет локального дебага
• Сложные chain-задачи
• Ручное управление памятью
• Проблемы с сериализацией
```

---

## **Проблемы с производительностью**

### **Small Files проблема**
```
📁 SMALL FILES 

Проблема: 10,000 маленьких файлов (1MB каждый)
         ↓
• 10,000 мапперов (неэффективно)
• Нагрузка на NameNode
• Большие накладные расходы

Решение:
• Объединение в SequenceFile
• Использование HAR
• Предобработка данных
```

### **Проблема shuffle**
```
🔄 SHUFFLE - УЗКОЕ ГОРЛО

Map Phase → Sort → Copy → Merge → Reduce Phase
    ↓
• Сеть перегружена
• Дисковые операции
• Сортировка больших данных
• Проблемы с памятью
```

---

## **Когда MapReduce ОПРАВДАН**

### **Идеальные use-cases**
```
MAPREDUCE ОПРАВДАН КОГДА:

• Пакетная обработка TB/PB данных
• ETL пайплайны (раз в день/неделю)
• Обработка логов
• Построение индексов
• Анализ всего датасета
• One-time миграции данных
```

### **Архитектурные решения**
```
СОВРЕМЕННЫЙ ПОДХОД

Lambda Architecture:
Batch Layer (MapReduce) + Speed Layer (Spark/Flink)

Или:
MapReduce для ETL → Spark для аналитики
                  ↓
           Columnar Storage (Parquet)
                  ↓
        SQL-движки для запросов
```

---

## **Эволюция экосистемы**

### **Переход к современным инструментам**
```
ЭВОЛЮЦИЯ ОБРАБОТКИ ДАННЫХ

MapReduce (2004) → Spark (2014) → Flink (2015)
     ↓                   ↓              ↓
Пакетная           Память         Реальное
обработка          Stream         время

HDFS → Parquet/ORC
Hive → Spark SQL, Presto
```

### **Рекомендации по выбору**
```
 КОГДА ЧТО ВЫБИРАТЬ:

MapReduce: Исторические данные, ETL
Spark:     ML, Graph, Streaming
Flink:     Real-time processing
Presto:    Interactive queries
ClickHouse: Analytics OLAP
```

---

# **Практика**

### **1. `secondary_sort.py` - ВТОРИЧНАЯ СОРТИРОВКА**
```python
"""
 ТЕОРЕТИЧЕСКАЯ ОСНОВА: Вторичная сортировка (Secondary Sort)

ПРОБЛЕМА: Данные приходят вразброс:
("2023-01", "Books") → 4500
("2023-02", "Electronics") → 12000  
("2023-01", "Electronics") → 15000

РЕШЕНИЕ: Упорядочиваем по месяцам И по категориям:
("2023-01", "Books") → 4500
("2023-01", "Electronics") → 15000
("2023-02", "Books") → 5200

ТЕХНИКА:
- Составной ключ: (year_month, category)
- Двухэтапный reducer
- Сортировка категорий по убыванию выручки внутри месяца
"""
```
**Создаем `secondary_sort.py`:**
```python
#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class RealSecondarySort(MRJob):
    
    def mapper(self, _, line):
        if 'Transaction ID' in line:
            return
            
        parts = line.split(',')
        if len(parts) >= 9:
            try:
                date_str = parts[1].strip()
                category = parts[5].strip()
                total_amount = float(parts[8])
                
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                year_month = date_obj.strftime('%Y-%m')
                
                # Ключ: (год-месяц, категория) - это позволяет сортировать по обоим полям
                yield (year_month, category), total_amount
                
            except (ValueError, IndexError):
                pass

    def reducer(self, key, values):
        year_month, category = key
        total_sales = sum(values)
        # Группируем по году-месяцу, внутри группы категории уже отсортированы
        yield year_month, (category, total_sales)

    def final_reducer(self, year_month, category_sales):
        # Внутри каждого месяца категории приходят отсортированными
        sales_by_category = []
        for category, sales in category_sales:
            sales_by_category.append((category, sales))
        
        # Выводим отсортированные данные
        for category, sales in sorted(sales_by_category, key=lambda x: x[1], reverse=True):
            yield f"{year_month}_{category}", sales

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]

if __name__ == '__main__':
    RealSecondarySort.run()
```
Копируем скрипты в контейнер
`docker cp secondary_sort.py namenode:/scripts/`

Запускаем ВНУТРИ контейнера

```
docker-compose exec namenode bash
export PATH="/tmp/python/bin:$PATH"
cd /scripts
```

Запускаем ВТОРИЧНУЮ СОРТИРОВКУ
```
python3 secondary_sort.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/secondary_sort
``` 
<details>
  <summary>Визуализация</summary>
    
### **`visualize_secondary_sort.py` - ВТОРИЧНАЯ СОРТИРОВКА**
```python
#!/usr/bin/env python3
import subprocess
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
from collections import defaultdict

def get_all_secondary_sort_data():
    cmd = "hdfs dfs -cat /user/root/output/secondary_sort/part-*"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    data = []
    for line in result.stdout.strip().split('\n'):
        if '\t' in line and 'INFO' not in line:
            key, value = line.split('\t')
            try:
                key_clean = key.strip('"')
                if '_' in key_clean:
                    year_month, category = key_clean.split('_', 1)
                    sales = float(value)
                    data.append({
                        'year_month': year_month,
                        'category': category,
                        'sales': sales
                    })
            except:
                continue
    return data

def create_combined_analysis():
    print("Сбор данных из всех файлов...")
    data = get_all_secondary_sort_data()
    
    if not data:
        print("Нет данных для анализа")
        return
    
    df = pd.DataFrame(data)
    
    # === ТЕКСТОВАЯ СТАТИСТИКА ===
    print("\n" + "="*80)
    print("ТЕКСТОВАЯ СТАТИСТИКА ВТОРИЧНОЙ СОРТИРОВКИ")
    print("="*80)
    
    # Общая статистика
    total_revenue = df['sales'].sum()
    total_months = df['year_month'].nunique()
    categories_count = df['category'].nunique()
    
    print(f"ОБЩАЯ СТАТИСТИКА:")
    print(f"• Записей: {len(data)}")
    print(f"• Месяцев: {total_months}") 
    print(f"• Категорий: {categories_count}")
    print(f"• Выручка: ${total_revenue:,.2f}")
    print()
    
    # Статистика по категориям
    category_totals = df.groupby('category')['sales'].sum().sort_values(ascending=False)
    
    print("СТАТИСТИКА ПО КАТЕГОРИЯМ:")
    print("-" * 50)
    for i, (category, revenue) in enumerate(category_totals.items(), 1):
        share = (revenue / total_revenue) * 100
        print(f"{i}. {category}: ${revenue:,.2f} ({share:.1f}%)")
    print()
    
    # Лидеры по месяцам
    monthly_leaders = df.loc[df.groupby('year_month')['sales'].idxmax()]
    
    print("ЛИДЕРЫ ПО МЕСЯЦАМ:")
    print("-" * 50)
    for _, row in monthly_leaders.sort_values('year_month').iterrows():
        print(f"{row['year_month']}: {row['category']} (${row['sales']:,.2f})")
    
    # === СОЗДАЕМ ГРАФИК ===
    print("\nСоздание графика...")
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('АНАЛИЗ ВТОРИЧНОЙ СОРТИРОВКИ: Продажи по месяцам и категориям', 
                fontsize=16, fontweight='bold')
    
    # 1. Heatmap
    pivot_sales = df.pivot_table(index='year_month', columns='category', 
                                values='sales', aggfunc='sum').fillna(0)
    pivot_sales = pivot_sales.sort_index()
    
    im = ax1.imshow(pivot_sales.values, cmap='YlOrRd', aspect='auto')
    ax1.set_xticks(range(len(pivot_sales.columns)))
    ax1.set_yticks(range(len(pivot_sales.index)))
    ax1.set_xticklabels(pivot_sales.columns, rotation=45, ha='right')
    ax1.set_yticklabels(pivot_sales.index)
    ax1.set_title('Тепловая карта продаж\n(вторичная сортировка по месяцам)')
    
    for i in range(len(pivot_sales.index)):
        for j in range(len(pivot_sales.columns)):
            if pivot_sales.iloc[i, j] > 0:
                ax1.text(j, i, f'{pivot_sales.iloc[i, j]/1000:.0f}K', 
                        ha="center", va="center", color="black", fontsize=8)
    
    plt.colorbar(im, ax=ax1, label='Выручка ($)')
    
    # 2. Доли категорий
    ax2.pie(category_totals.values, labels=category_totals.index, autopct='%1.1f%%',
           startangle=90, colors=['#ff6b6b', '#4ecdc4', '#45b7d1'])
    ax2.set_title('Распределение выручки по категориям')
    
    # 3. Динамика топ категорий
    top_categories = category_totals.head(2).index
    for category in top_categories:
        category_data = df[df['category'] == category].sort_values('year_month')
        ax3.plot(category_data['year_month'], category_data['sales'], 
                marker='o', linewidth=2, label=category)
    
    ax3.set_title('Динамика топ-2 категорий')
    ax3.set_ylabel('Выручка ($)')
    ax3.legend()
    ax3.tick_params(axis='x', rotation=45)
    ax3.grid(True, alpha=0.3)
    
    # 4. Статистика лидеров
    leader_counts = monthly_leaders['category'].value_counts()
    bars = ax4.bar(leader_counts.index, leader_counts.values, 
                  color=['#ff9999', '#66b3ff', '#99ff99'])
    ax4.set_title('Количество месяцев в лидерах\nпо категориям')
    ax4.set_ylabel('Месяцев')
    
    for bar, count in zip(bars, leader_counts.values):
        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                f'{count}', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('/scripts/combined_analysis.png', dpi=100, bbox_inches='tight')
    plt.close()
    
    print(f"\nГрафик сохранен: combined_analysis.png")
    print(f"Общая выручка: ${total_revenue:,.2f}")
    print(f"Топ категория: {category_totals.index[0]} (${category_totals.iloc[0]:,.2f})")

if __name__ == '__main__':
    create_combined_analysis()
```
```bash
# 1. Удаляем старые результаты, если они есть
docker-compose exec namenode hdfs dfs -rm -r /user/root/output/secondary_sort

# 2. Копируем скрипты в контейнер
docker cp secondary_sort.py namenode:/scripts/
docker cp visualize_secondary_sort.py namenode:/scripts/

# 3. Запускаем анализ
docker-compose exec namenode bash
cd /scripts

python3 secondary_sort.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/secondary_sort

# 4. Запускаем визуализацию
python3 visualize_secondary_sort.py

# 5. Проверяем результаты
hdfs dfs -cat /user/root/output/secondary_sort/part-00000 | head -10

# 6. Копируем график на хост
docker cp namenode:/scripts/combined_analysis.png ./

# 7. Смотрим график
feh combined_analysis.png
```
</details>

### **2. `composite_keys.py` - СОСТАВНЫЕ КЛЮЧИ**
```python
"""
ТЕОРЕТИЧЕСКАЯ ОСНОВА: Составные ключи для многомерного анализа

ПРОБЛЕМА: Традиционный подход требует нескольких проходов:
1. Анализ по полу → 1 Job
2. Анализ по возрасту → 2 Job  
3. Анализ по категориям → 3 Job

РЕШЕНИЕ: Единый проход с составными ключами:
"CROSS_GENDER_CATEGORY_Male_Electronics" → 45000
"CROSS_AGE_CATEGORY_25-34_Books" → 15000
"TRIPLE_Female_25-34_Electronics" → 12000

ТЕХНИКА:
- Ключи как измерения: DEMO_GENDER_, PRODUCT_, CROSS_
- Кросс-секционный анализ в одном mapper
- Избежание повторных проходов по данным
"""
```
**Создаем `composite_keys.py`:**
```python
#!/usr/bin/env python3
from mrjob.job import MRJob
from datetime import datetime

class CompositeKeysAnalysis(MRJob):

    def mapper(self, _, line):
        if 'Transaction ID' in line:
            return
            
        parts = line.split(',')
        if len(parts) >= 9:
            try:
                date_str = parts[1].strip()
                gender = parts[3].strip()
                age = int(parts[4])
                category = parts[5].strip()
                total_amount = float(parts[8])
                
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                year_month = date_obj.strftime('%Y-%m')
                age_group = self.get_age_group(age)
                season = self.get_season(date_obj.month)
                
                # СОСТАВНЫЕ КЛЮЧИ - многомерный анализ в одном проходе
                
                # Временные срезы
                yield f"TIME_{year_month}", total_amount
                yield f"TIME_SEASON_{season}", total_amount
                
                # Демографические срезы  
                yield f"DEMO_GENDER_{gender}", total_amount
                yield f"DEMO_AGE_{age_group}", total_amount
                
                # Продуктовые срезы
                yield f"PRODUCT_{category}", total_amount
                
                # КРОСС-СЕКЦИОННЫЕ АНАЛИЗЫ (составные ключи)
                yield f"CROSS_GENDER_CATEGORY_{gender}_{category}", total_amount
                yield f"CROSS_AGE_CATEGORY_{age_group}_{category}", total_amount
                yield f"CROSS_SEASON_CATEGORY_{season}_{category}", total_amount
                yield f"CROSS_GENDER_AGE_{gender}_{age_group}", total_amount
                
                # Тройные пересечения
                yield f"TRIPLE_{gender}_{age_group}_{category}", total_amount
                
            except (ValueError, IndexError) as e:
                self.increment_counter('errors', 'parsing_error', 1)

    def get_age_group(self, age):
        if age <= 24: return "18-24"
        elif age <= 34: return "25-34" 
        elif age <= 44: return "35-44"
        elif age <= 54: return "45-54"
        else: return "55+"

    def get_season(self, month):
        if month in [12, 1, 2]: return "WINTER"
        elif month in [3, 4, 5]: return "SPRING"
        elif month in [6, 7, 8]: return "SUMMER"
        else: return "AUTUMN"

    def reducer(self, key, values):
        total = sum(values)
        count = sum(1 for _ in values)
        
        if key.startswith("TRIPLE"):
            yield key, f"${total:,.2f} ({count} покупок)"
        else:
            yield key, f"${total:,.2f}"

if __name__ == '__main__':
    CompositeKeysAnalysis.run()
```
<details>
  <summary>Визуализация</summary>

### **`visualize_composite_keys.py`
```python
#!/usr/bin/env python3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import subprocess
import numpy as np
import pandas as pd

def main():
    print("МНОГОМЕРНЫЙ АНАЛИЗ: Составные ключи")
    
    # Получаем ВСЕ данные
    cmd = "hdfs dfs -cat /user/root/output/composite_keys/part-*"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    # Собираем многомерные данные с ПРАВИЛЬНЫМ парсингом
    gender_age_data = []
    age_category_data = []
    gender_category_data = []
    season_category_data = []
    
    for line in result.stdout.strip().split('\n'):
        if '\t' in line and 'INFO' not in line:
            key, value = line.split('\t')
            key = key.strip('"')
            value = value.strip('"')
            
            try:
                # Парсим только числовые значения с $
                if value.startswith('$'):
                    amount = float(value.replace('$', '').replace(',', ''))
                    
                    # CROSS_GENDER_AGE - пол × возраст
                    if key.startswith('CROSS_GENDER_AGE_'):
                        parts = key.split('_')
                        if len(parts) >= 5:
                            gender = parts[3]  # Female
                            age_group = parts[4]  # 18-24
                            gender_age_data.append({
                                'gender': gender,
                                'age_group': age_group,
                                'amount': amount
                            })
                    
                    # CROSS_AGE_CATEGORY - возраст × категория
                    elif key.startswith('CROSS_AGE_CATEGORY_'):
                        parts = key.split('_')
                        if len(parts) >= 5:
                            age_group = parts[3]  # 18-24
                            category = parts[4]   # Beauty
                            age_category_data.append({
                                'age_group': age_group,
                                'category': category,
                                'amount': amount
                            })
                    
                    # CROSS_GENDER_CATEGORY - пол × категория
                    elif key.startswith('CROSS_GENDER_CATEGORY_'):
                        parts = key.split('_')
                        if len(parts) >= 5:
                            gender = parts[3]  # Female
                            category = parts[4]  # Beauty
                            gender_category_data.append({
                                'gender': gender,
                                'category': category,
                                'amount': amount
                            })
                    
                    # CROSS_SEASON_CATEGORY - сезон × категория
                    elif key.startswith('CROSS_SEASON_CATEGORY_'):
                        parts = key.split('_')
                        if len(parts) >= 5:
                            season = parts[3]  # AUTUMN
                            category = parts[4]  # Beauty
                            season_category_data.append({
                                'season': season,
                                'category': category,
                                'amount': amount
                            })
                        
            except:
                continue
    
    # Создаем комплексные многомерные графики
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('МНОГОМЕРНЫЙ АНАЛИЗ: Пересечения демографии и поведения', 
                fontsize=16, fontweight='bold')
    
    # 1. Heatmap: Пол × Возраст
    if gender_age_data:
        df = pd.DataFrame(gender_age_data)
        pivot = df.pivot_table(index='gender', columns='age_group', values='amount', aggfunc='sum').fillna(0)
        
        # Сортируем возрастные группы
        age_order = ['18-24', '25-34', '35-44', '45-54', '55+']
        pivot = pivot[age_order]
        
        im1 = ax1.imshow(pivot.values, cmap='YlOrRd', aspect='auto')
        ax1.set_xticks(range(len(age_order)))
        ax1.set_yticks(range(len(pivot.index)))
        ax1.set_xticklabels(age_order)
        ax1.set_yticklabels(pivot.index)
        ax1.set_title('ПОЛ × ВОЗРАСТ\nВыручка по демографическим группам')
        ax1.set_xlabel('Возрастные группы')
        ax1.set_ylabel('Пол')
        
        for i in range(len(pivot.index)):
            for j in range(len(age_order)):
                value = pivot.iloc[i, j]
                if value > 0:
                    ax1.text(j, i, f'${value/1000:.0f}K', 
                            ha="center", va="center", color="black", fontsize=10,
                            fontweight='bold')
        
        plt.colorbar(im1, ax=ax1, label='Выручка ($)')
    
    # 2. Heatmap: Возраст × Категория
    if age_category_data:
        df = pd.DataFrame(age_category_data)
        pivot = df.pivot_table(index='age_group', columns='category', values='amount', aggfunc='sum').fillna(0)
        
        # Сортируем возрастные группы
        age_order = ['18-24', '25-34', '35-44', '45-54', '55+']
        pivot = pivot.reindex(age_order)
        
        im2 = ax2.imshow(pivot.values, cmap='Blues', aspect='auto')
        ax2.set_xticks(range(len(pivot.columns)))
        ax2.set_yticks(range(len(age_order)))
        ax2.set_xticklabels(pivot.columns, rotation=45, ha='right')
        ax2.set_yticklabels(age_order)
        ax2.set_title('ВОЗРАСТ × КАТЕГОРИЯ\nПредпочтения по возрастам')
        ax2.set_xlabel('Категории товаров')
        ax2.set_ylabel('Возрастные группы')
        
        for i in range(len(age_order)):
            for j in range(len(pivot.columns)):
                value = pivot.iloc[i, j]
                if value > 0:
                    ax2.text(j, i, f'${value/1000:.0f}K', 
                            ha="center", va="center", color="black", fontsize=9)
        
        plt.colorbar(im2, ax=ax2, label='Выручка ($)')
    
    # 3. Grouped bar: Пол × Категория
    if gender_category_data:
        df = pd.DataFrame(gender_category_data)
        pivot = df.pivot_table(index='gender', columns='category', values='amount', aggfunc='sum').fillna(0)
        
        categories = pivot.columns
        x = np.arange(len(pivot.index))
        width = 0.25
        
        for i, category in enumerate(categories):
            offset = width * i
            values = pivot[category].values
            ax3.bar(x + offset, values, width, label=category,
                   color=plt.cm.Set3(i / len(categories)))
            
            for j, value in enumerate(values):
                ax3.text(j + offset, value + 1000, f'${value/1000:.0f}K',
                        ha='center', va='bottom', fontsize=8, fontweight='bold')
        
        ax3.set_xticks(x + width)
        ax3.set_xticklabels(pivot.index)
        ax3.set_title('ПОЛ × КАТЕГОРИЯ\nПредпочтения по полу')
        ax3.set_ylabel('Выручка ($)')
        ax3.legend(title='Категории')
    
    # 4. Stacked bar: Сезон × Категория
    if season_category_data:
        df = pd.DataFrame(season_category_data)
        pivot = df.pivot_table(index='season', columns='category', values='amount', aggfunc='sum').fillna(0)
        
        categories = pivot.columns
        x = range(len(pivot.index))
        bottom = np.zeros(len(pivot.index))
        
        for i, category in enumerate(categories):
            values = pivot[category].values
            ax4.bar(x, values, bottom=bottom, label=category, 
                   color=plt.cm.Pastel1(i / len(categories)))
            bottom += values
        
        ax4.set_xticks(x)
        ax4.set_xticklabels(pivot.index)
        ax4.set_title('СЕЗОН × КАТЕГОРИЯ\nСезонные предпочтения')
        ax4.set_ylabel('Выручка ($)')
        ax4.legend(title='Категории')
        
        # Добавляем общие суммы
        for i, season in enumerate(pivot.index):
            total = pivot.loc[season].sum()
            ax4.text(i, total + 2000, f'${total/1000:.0f}K', 
                    ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('/scripts/composite_keys_analysis.png', dpi=100, bbox_inches='tight')
    plt.close()
    
    print("График сохранен: composite_keys_analysis.png")
    
    # Ключевые инсайты
    print(f"\nКЛЮЧЕВЫЕ ИНСАЙТЫ:")
    
    if gender_age_data:
        df = pd.DataFrame(gender_age_data)
        max_combo = df.loc[df['amount'].idxmax()]
        print(f"• Самые активные: {max_combo['gender']} {max_combo['age_group']} (${max_combo['amount']:,.0f})")
    
    if age_category_data:
        df = pd.DataFrame(age_category_data)
        max_combo = df.loc[df['amount'].idxmax()]
        print(f"• Самый прибыльный сегмент: {max_combo['age_group']} покупают {max_combo['category']} (${max_combo['amount']:,.0f})")

if __name__ == '__main__':
    main()
```
# 1. Копируем скрипты в контейнер
docker cp composite_keys.py namenode:/scripts/
docker cp visualize_composite_keys.py namenode:/scripts/

# 2. Запускаем анализ
docker-compose exec namenode bash
cd /scripts

python3 composite_keys.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/composite_keys

# 4. Запускаем визуализацию
python3 visualize_composite_keys.py

# 5. Проверяем результаты
hdfs dfs -cat /user/root/output/composite_keys/part-00000 | head -10

# 6. Копируем график на хост
docker cp namenode:/scripts/composite_keys_analysis.png ./

# 7. Смотрим график
feh composite_keys_analysis.png

</details>

### **3. `multiple_outputs.py` - MULTIPLE OUTPUTS**
```python
"""
ТЕОРЕТИЧЕСКАЯ ОСНОВА: Multiple Outputs - единый проход для всей аналитики

ПРОБЛЕМА: Разные типы аналитики требуют разных форматов вывода

РЕШЕНИЕ: Единый Mapper → Multiple Outputs:
├── TREND_MONTHLY_2023-01 → $45,000
├── DEMO_GENDER_Male → $150,000  
├── PRODUCT_Electronics_REVENUE → $90,000
├── METRIC_AVG_RECEIPT → $85.20
└── SEGMENT_HIGH_VALUE_Male_25-34 → $45,000

ТЕХНИКА:
- Разные префиксы ключей = разные типы аналитики
- Единый проход по данным
- Раздельная обработка в reducer
"""
```
**Создаем `multiple_outputs.py`:**
```python
#!/usr/bin/env python3
from mrjob.job import MRJob
from datetime import datetime
import json

class MultipleOutputsAnalysis(MRJob):

    def mapper(self, _, line):
        if 'Transaction ID' in line:
            return
            
        parts = line.split(',')
        if len(parts) >= 9:
            try:
                date_str = parts[1].strip()
                gender = parts[3].strip()
                age = int(parts[4])
                category = parts[5].strip()
                quantity = int(parts[6])
                price_per_unit = float(parts[7])
                total_amount = float(parts[8])
                
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                year_month = date_obj.strftime('%Y-%m')
                age_group = self.get_age_group(age)
                
                # MULTIPLE OUTPUTS В ОДНОМ MAPPER
                
                # 1. ВЫХОД: Временные тренды
                yield f"TREND_MONTHLY_{year_month}", total_amount
                yield f"TREND_MONTHLY_COUNT_{year_month}", 1
                
                # 2. ВЫХОД: Демография
                yield f"DEMO_GENDER_{gender}", total_amount
                yield f"DEMO_AGE_{age_group}", total_amount
                
                # 3. ВЫХОД: Продуктовый анализ
                yield f"PRODUCT_{category}_REVENUE", total_amount
                yield f"PRODUCT_{category}_QUANTITY", quantity
                yield f"PRODUCT_{category}_AVG_PRICE", price_per_unit
                
                # 4. ВЫХОД: Метрики эффективности
                yield f"METRIC_AVG_RECEIPT", total_amount
                yield f"METRIC_TOTAL_QUANTITY", quantity
                yield f"METRIC_UNIQUE_CATEGORIES", category
                
                # 5. ВЫХОД: Сегменты покупателей
                if total_amount > 200:
                    yield f"SEGMENT_HIGH_VALUE_{gender}_{age_group}", total_amount
                elif total_amount > 100:
                    yield f"SEGMENT_MEDIUM_VALUE_{gender}_{age_group}", total_amount
                else:
                    yield f"SEGMENT_LOW_VALUE_{gender}_{age_group}", total_amount
                    
            except (ValueError, IndexError) as e:
                self.increment_counter('errors', 'parsing_error', 1)

    def get_age_group(self, age):
        if age <= 24: return "18-24"
        elif age <= 34: return "25-34"
        elif age <= 44: return "35-44"
        elif age <= 54: return "45-54"
        else: return "55+"

    def reducer(self, key, values):
        values_list = list(values)
        
        if "COUNT" in key:
            count = sum(values_list)
            yield key, count
        elif "AVG_PRICE" in key or "AVG_RECEIPT" in key:
            avg = sum(values_list) / len(values_list)
            yield key, f"${avg:.2f}"
        elif "UNIQUE" in key:
            unique_count = len(set(values_list))
            yield key, unique_count
        else:
            total = sum(values_list)
            yield key, f"${total:,.2f}"

if __name__ == '__main__':
    MultipleOutputsAnalysis.run()
```
<details>
  <summary>Визуализация</summary>
    
** `visualize_multiple_outputs.py` - MULTIPLE OUTPUTS**
```python
#!/usr/bin/env python3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import subprocess

def get_multiple_outputs_results():
    cmd = "hdfs dfs -cat /user/root/output/multiple_outputs/part-*"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    data = {}
    for line in result.stdout.strip().split('\n'):
        if '\t' in line:
            key, value = line.split('\t')
            data[key] = value
    return data

def visualize_multiple_outputs():
    print("ВИЗУАЛИЗАЦИЯ: Multiple Outputs")
    print("Теоретическая основа: Комплексная аналитика из одного Job")
    
    results = get_multiple_outputs_results()
    
    # Группируем по типам выходных данных
    trend_data = {k: v for k, v in results.items() if k.startswith('TREND_')}
    demo_data = {k: v for k, v in results.items() if k.startswith('DEMO_')}
    product_data = {k: v for k, v in results.items() if k.startswith('PRODUCT_')}
    metric_data = {k: v for k, v in results.items() if k.startswith('METRIC_')}
    segment_data = {k: v for k, v in results.items() if k.startswith('SEGMENT_')}
    
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('MULTIPLE OUTPUTS: Комплексная аналитика из одного Job', 
                fontsize=16, fontweight='bold')
    
    # 1. Тренды
    if trend_data:
        trend_df = pd.DataFrame(list(trend_data.items()), columns=['trend', 'value'])
        axes[0,0].bar(range(len(trend_df)), [float(str(v).replace('$', '').replace(',', '')) 
                                           for v in trend_df['value']], color='skyblue')
        axes[0,0].set_title('Временные тренды')
        axes[0,0].set_ylabel('Значение')
    
    # 2. Демография
    if demo_data:
        demo_df = pd.DataFrame(list(demo_data.items()), columns=['demo', 'value'])
        axes[0,1].pie([float(str(v).replace('$', '').replace(',', '')) for v in demo_df['value']], 
                     labels=demo_df['demo'].str.replace('DEMO_', ''), autopct='%1.1f%%')
        axes[0,1].set_title('Демографическое распределение')
    
    # 3. Продукты
    if product_data:
        product_df = pd.DataFrame(list(product_data.items()), columns=['product', 'value'])
        axes[0,2].barh(range(len(product_df)), 
                      [float(str(v).replace('$', '').replace(',', '')) for v in product_df['value']],
                      color='lightgreen')
        axes[0,2].set_title('Продуктовый анализ')
        axes[0,2].set_xlabel('Выручка ($)')
    
    # 4. Метрики
    if metric_data:
        metric_df = pd.DataFrame(list(metric_data.items()), columns=['metric', 'value'])
        axes[1,0].bar(range(len(metric_df)), [float(str(v).replace('$', '')) for v in metric_df['value']],
                     color='gold')
        axes[1,0].set_title('Бизнес-метрики')
        axes[1,0].set_ylabel('Значение')
    
    # 5. Сегменты
    if segment_data:
        segment_counts = {}
        for key in segment_data.keys():
            segment_type = key.split('_')[1]
            segment_counts[segment_type] = segment_counts.get(segment_type, 0) + 1
        
        axes[1,1].bar(segment_counts.keys(), segment_counts.values(), color='lightcoral')
        axes[1,1].set_title('Сегментация клиентов')
        axes[1,1].set_ylabel('Количество сегментов')
    
    # 6. Сводная информация
    axes[1,2].text(0.1, 0.9, 'СВОДКА MULTIPLE OUTPUTS:', fontsize=12, fontweight='bold')
    axes[1,2].text(0.1, 0.7, f'Тренды: {len(trend_data)}', fontsize=10)
    axes[1,2].text(0.1, 0.6, f'Демография: {len(demo_data)}', fontsize=10)
    axes[1,2].text(0.1, 0.5, f'Продукты: {len(product_data)}', fontsize=10)
    axes[1,2].text(0.1, 0.4, f'Метрики: {len(metric_data)}', fontsize=10)
    axes[1,2].text(0.1, 0.3, f'Сегменты: {len(segment_data)}', fontsize=10)
    axes[1,2].axis('off')
    axes[1,2].set_title('Статистика выходных данных')
    
    plt.tight_layout()
    plt.savefig('/scripts/multiple_outputs_analysis.png', dpi=100, bbox_inches='tight')
    plt.close()
    
    print("График сохранен: multiple_outputs_analysis.png")
    
    print("\n" + "="*80)
    print("АНАЛИЗ MULTIPLE OUTPUTS")
    print("="*80)
    print(f"Всего сгенерировано {len(results)} различных метрик")
    print(f"Типы анализа: Тренды ({len(trend_data)}), Демография ({len(demo_data)}), "
          f"Продукты ({len(product_data)}), Метрики ({len(metric_data)}), "
          f"Сегменты ({len(segment_data)})")

if __name__ == '__main__':
    visualize_multiple_outputs()
 ```
```bash
docker cp multiple_outputs.py namenode:/scripts/
docker cp visualize_multiple_outputs.py namenode:/scripts/ 

python3 multiple_outputs.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/multiple_outputs
  
python3 visualize_multiple_outputs.py

docker cp namenode:/scripts/multiple_outputs_analysis.png ./

feh multiple_outputs_analysis.png
```
</details>

### **4. `real_price_elasticity.py` - ЦЕНОВАЯ ЭЛАСТИЧНОСТЬ**
```python
"""
ТЕОРЕТИЧЕСКАЯ ОСНОВА: Сложные бизнес-метрики

ПРОБЛЕМА: Простой анализ не показывает зависимость спроса от цены

РЕШЕНИЕ: Анализ ценовой эластичности:
"ELASTICITY_Electronics_PRICE" → {"avg": 85.50, "min": 25, "max": 299}
"ELASTICITY_Electronics_QUANTITY" → "2.1 ед."
"SEGMENT_PRICE_Electronics_PREMIUM" → $45,200

ТЕХНИКА:
- Статистические агрегаты (mean, min, max)
- Сегментация: BUDGET/STANDARD/PREMIUM/LUXURY
- Анализ объемов: SINGLE/SMALL/MEDIUM/BULK
- Соотношение цена/количество
"""
```
**Создаем `real_price_elasticity.py`:**
```python
#!/usr/bin/env python3
from mrjob.job import MRJob
import statistics

class RealPriceElasticity(MRJob):

    def mapper(self, _, line):
        if 'Transaction ID' in line:
            return
            
        parts = line.split(',')
        if len(parts) >= 9:
            try:
                category = parts[5].strip()
                quantity = int(parts[6])
                price_per_unit = float(parts[7])
                total_amount = float(parts[8])
                
                # Анализ ценовых сегментов и поведения
                price_segment = self.get_price_segment(price_per_unit)
                quantity_segment = self.get_quantity_segment(quantity)
                
                # Эластичность: как количество меняется с ценой
                yield f"ELASTICITY_{category}_PRICE", price_per_unit
                yield f"ELASTICITY_{category}_QUANTITY", quantity
                yield f"ELASTICITY_{category}_REVENUE", total_amount
                
                # Анализ по ценовым сегментам
                yield f"SEGMENT_PRICE_{category}_{price_segment}", total_amount
                yield f"SEGMENT_PRICE_COUNT_{category}_{price_segment}", 1
                
                # Анализ объемов покупок
                yield f"SEGMENT_QUANTITY_{category}_{quantity_segment}", total_amount
                
                # Соотношение цена/количество
                if quantity > 0:
                    yield f"PRICE_PER_UNIT_{category}", price_per_unit
                    
            except (ValueError, IndexError) as e:
                self.increment_counter('errors', 'parsing_error', 1)

    def get_price_segment(self, price):
        if price <= 20: return "BUDGET"
        elif price <= 50: return "STANDARD"
        elif price <= 100: return "PREMIUM"
        else: return "LUXURY"

    def get_quantity_segment(self, quantity):
        if quantity == 1: return "SINGLE"
        elif quantity <= 3: return "SMALL"
        elif quantity <= 5: return "MEDIUM"
        else: return "BULK"

    def reducer(self, key, values):
        values_list = list(values)
        
        if "ELASTICITY" in key:
            if "PRICE" in key:
                stats = {
                    'avg': statistics.mean(values_list),
                    'min': min(values_list),
                    'max': max(values_list),
                    'count': len(values_list)
                }
                yield key, stats
            elif "QUANTITY" in key:
                avg_quantity = statistics.mean(values_list)
                yield key, f"{avg_quantity:.1f} ед."
            else:
                total = sum(values_list)
                yield key, f"${total:,.2f}"
                
        elif "COUNT" in key:
            count = sum(values_list)
            yield key, count
        elif "PRICE_PER_UNIT" in key:
            avg_price = statistics.mean(values_list)
            yield key, f"${avg_price:.2f}"
        else:
            total = sum(values_list)
            yield key, f"${total:,.2f}"

if __name__ == '__main__':
    RealPriceElasticity.run()
```
<details>
  <summary>Визуализация</summary>

**4. `visualize_real_price_elasticity.py` - ЦЕНОВАЯ ЭЛАСТИЧНОСТЬ**
```python
#!/usr/bin/env python3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import subprocess
import numpy as np
import json

def get_price_data():
    cmd = "hdfs dfs -cat /user/root/output/price_elasticity/part-*"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    data = {}
    for line in result.stdout.strip().split('\n'):
        if '\t' in line and 'INFO' not in line:
            key, value = line.split('\t')
            try:
                key_clean = key.strip('"')
                value_clean = value.strip().strip('"')
                
                # Парсим JSON данные
                if value_clean.startswith('{'):
                    try:
                        # Исправляем JSON (добавляем запятые)
                        value_fixed = value_clean.replace('" ', '", "')
                        data[key_clean] = json.loads(value_fixed)
                    except:
                        data[key_clean] = value_clean
                
                # Парсим числа с долларом
                elif value_clean.startswith('$'):
                    amount = float(value_clean.replace('$', '').replace(',', ''))
                    data[key_clean] = amount
                
                # Парсим обычные числа
                else:
                    try:
                        amount = float(value_clean)
                        data[key_clean] = amount
                    except:
                        data[key_clean] = value_clean
                        
            except:
                continue
    return data

def main():
    print("ВИЗУАЛИЗАЦИЯ: Ценовая эластичность")
    
    data = get_price_data()
    
    if not data:
        print("Нет данных")
        return
    
    # Извлекаем средние цены из JSON
    avg_prices = {}
    for key, value in data.items():
        if 'ELASTICITY' in key and 'PRICE' in key and isinstance(value, dict):
            category = key.replace('ELASTICITY_', '').replace('_PRICE', '')
            avg_prices[category] = value.get('avg', 0)
    
    # Извлекаем данные по ценовым сегментам
    segment_data = {}
    segment_counts = {}
    
    for key, value in data.items():
        if 'SEGMENT_PRICE_' in key and 'COUNT' not in key and isinstance(value, (int, float)):
            parts = key.split('_')
            if len(parts) >= 4:
                category = parts[2]
                segment = parts[3]
                if category not in segment_data:
                    segment_data[category] = {}
                segment_data[category][segment] = value
        
        elif 'SEGMENT_PRICE_COUNT_' in key and isinstance(value, (int, float)):
            parts = key.split('_')
            if len(parts) >= 5:
                category = parts[3]
                segment = parts[4]
                if category not in segment_counts:
                    segment_counts[category] = {}
                segment_counts[category][segment] = value
    
    # Создаем комплексный график
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('АНАЛИЗ ЦЕНОВОЙ ЭЛАСТИЧНОСТИ', fontsize=16, fontweight='bold')
    
    # 1. Средние цены по категориям
    if avg_prices:
        categories = list(avg_prices.keys())
        prices = list(avg_prices.values())
        
        bars = ax1.bar(categories, prices, color=['lightcoral', 'lightgreen', 'lightblue'])
        ax1.set_title('СРЕДНИЕ ЦЕНЫ ПО КАТЕГОРИЯМ')
        ax1.set_ylabel('Цена ($)')
        
        for i, (category, price) in enumerate(zip(categories, prices)):
            ax1.text(i, price + 5, f'${price:.1f}', ha='center', va='bottom', fontweight='bold')
    
    # 2. Выручка по ценовым сегментам (Beauty)
    if 'Beauty' in segment_data:
        segments = list(segment_data['Beauty'].keys())
        revenues = list(segment_data['Beauty'].values())
        total = sum(revenues)
        
        bars = ax2.bar(segments, revenues, color=['gold', 'lightcoral'])
        ax2.set_title('BEAUTY: Выручка по ценовым сегментам')
        ax2.set_ylabel('Выручка ($)')
        
        for i, (segment, revenue) in enumerate(zip(segments, revenues)):
            percentage = (revenue / total) * 100
            ax2.text(i, revenue + total*0.01, f'${revenue/1000:.0f}K\n({percentage:.1f}%)', 
                    ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # 3. Выручка по ценовым сегментам (Clothing)
    if 'Clothing' in segment_data:
        segments = list(segment_data['Clothing'].keys())
        revenues = list(segment_data['Clothing'].values())
        total = sum(revenues)
        
        bars = ax3.bar(segments, revenues, color=['gold', 'lightcoral'])
        ax3.set_title('CLOTHING: Выручка по ценовым сегментам')
        ax3.set_ylabel('Выручка ($)')
        
        for i, (segment, revenue) in enumerate(zip(segments, revenues)):
            percentage = (revenue / total) * 100
            ax3.text(i, revenue + total*0.01, f'${revenue/1000:.0f}K\n({percentage:.1f}%)', 
                    ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # 4. Выручка по ценовым сегментам (Electronics)
    if 'Electronics' in segment_data:
        segments = list(segment_data['Electronics'].keys())
        revenues = list(segment_data['Electronics'].values())
        total = sum(revenues)
        
        bars = ax4.bar(segments, revenues, color=['gold', 'lightcoral'])
        ax4.set_title('ELECTRONICS: Выручка по ценовым сегментам')
        ax4.set_ylabel('Выручка ($)')
        
        for i, (segment, revenue) in enumerate(zip(segments, revenues)):
            percentage = (revenue / total) * 100
            ax4.text(i, revenue + total*0.01, f'${revenue/1000:.0f}K\n({percentage:.1f}%)', 
                    ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('/scripts/price_elasticity_analysis.png', dpi=100, bbox_inches='tight')
    plt.close()
    
    print("График сохранен: price_elasticity_analysis.png")
    
    # Текстовая статистика
    print(f"\nАНАЛИЗ ЦЕНОВОЙ ЭЛАСТИЧНОСТИ")
    print("=" * 50)
    
    if avg_prices:
        print("СРЕДНИЕ ЦЕНЫ:")
        for category, price in avg_prices.items():
            print(f"  {category}: ${price:.2f}")
    
    print(f"\nВЫРУЧКА ПО ЦЕНОВЫМ СЕГМЕНТАМ:")
    for category in ['Beauty', 'Clothing', 'Electronics']:
        if category in segment_data:
            total = sum(segment_data[category].values())
            print(f"\n  {category}: ${total:,.2f}")
            for segment, revenue in segment_data[category].items():
                percentage = (revenue / total) * 100
                count = segment_counts.get(category, {}).get(segment, 0)
                print(f"    {segment}: ${revenue:,.2f} ({percentage:.1f}%, {count} транзакций)")

if __name__ == '__main__':
    main()
```
```bash
docker cp real_price_elasticity.py namenode:/scripts/
docker cp visualize_real_price_elasticity.py namenode:/scripts/

python3 real_price_elasticity.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/price_elasticity

python3 visualize_real_price_elasticity.py
docker cp namenode:/scripts/price_elasticity_analysis.png ./
feh price_elasticity_analysis.png
```
</details>

### **5. `demographic_category_analysis.py` - МНОГОМЕРНАЯ ГРУППИРОВКА**
```python
"""
ТЕОРЕТИЧЕСКАЯ ОСНОВА: Группировка по возрасту, полу и категориям

ПРОБЛЕМА: Простые группировки не показывают пересечения

РЕШЕНИЕ: Многомерная группировка:
"GENDER_CATEGORY_Male_Electronics" → $45,000
"AGE_CATEGORY_25-34_Books" → $15,000  
"GENDER_AGE_CATEGORY_Female_35-44_Clothing" → $28,000

ТЕХНИКА:
- Двойные и тройные группировки
- Иерархические ключи
- Анализ пересечений демографии и продуктов
"""
```
**Создаем `demographic_category_analysis.py`:**
```python
#!/usr/bin/env python3
from mrjob.job import MRJob
from collections import defaultdict

class DemographicCategoryAnalysis(MRJob):

    def mapper(self, _, line):
        if 'Transaction ID' in line:
            return
            
        parts = line.split(',')
        if len(parts) >= 9:
            try:
                gender = parts[3].strip()
                age = int(parts[4])
                category = parts[5].strip()
                total_amount = float(parts[8])
                
                age_group = self.get_age_group(age)
                
                # МНОГОМЕРНАЯ ГРУППИРОВКА
                
                # 1. Пол + Категория
                yield f"GENDER_CATEGORY_{gender}_{category}", total_amount
                yield f"GENDER_CATEGORY_COUNT_{gender}_{category}", 1
                
                # 2. Возраст + Категория  
                yield f"AGE_CATEGORY_{age_group}_{category}", total_amount
                yield f"AGE_CATEGORY_COUNT_{age_group}_{category}", 1
                
                # 3. Пол + Возраст + Категория (тройная группировка)
                yield f"GENDER_AGE_CATEGORY_{gender}_{age_group}_{category}", total_amount
                yield f"GENDER_AGE_CATEGORY_COUNT_{gender}_{age_group}_{category}", 1
                
                # 4. Общие демографические метрики
                yield f"DEMO_GENDER_{gender}", total_amount
                yield f"DEMO_AGE_{age_group}", total_amount
                yield f"DEMO_CATEGORY_{category}", total_amount
                
            except (ValueError, IndexError) as e:
                self.increment_counter('errors', 'parsing_error', 1)

    def get_age_group(self, age):
        if age <= 24: return "18-24"
        elif age <= 34: return "25-34"
        elif age <= 44: return "35-44"
        elif age <= 54: return "45-54"
        else: return "55+"

    def reducer(self, key, values):
        values_list = list(values)
        
        if "COUNT" in key:
            count = sum(values_list)
            yield key, count
        else:
            total = sum(values_list)
            avg = total / len(values_list) if "AVG" not in key else total
            
            if "GENDER_AGE_CATEGORY" in key:
                yield key, f"${total:,.2f} (ср. ${avg:.2f})"
            else:
                yield key, f"${total:,.2f}"

if __name__ == '__main__':
    DemographicCategoryAnalysis.run()
```
<details>
  <summary>Визуализация</summary>

**5. `visualize_demographic_category.py` - ДЕМОГРАФИЯ + КАТЕГОРИИ**
```python
#!/usr/bin/env python3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import subprocess
import numpy as np

def get_demographic_data():
    cmd = "hdfs dfs -cat /user/root/output/demographic_category/part-*"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    data = []
    for line in result.stdout.strip().split('\n'):
        if '\t' in line and 'INFO' not in line:
            key, value = line.split('\t')
            try:
                key_clean = key.strip('"')
                value_clean = value.strip().strip('"')
                
                # Парсим числовые значения
                if value_clean.startswith('$'):
                    amount = float(value_clean.replace('$', '').replace(',', ''))
                    data.append({'key': key_clean, 'value': amount, 'type': 'revenue'})
                else:
                    # Пробуем как число (для COUNT)
                    try:
                        amount = float(value_clean)
                        data.append({'key': key_clean, 'value': amount, 'type': 'count'})
                    except:
                        pass
                        
            except:
                continue
    return data

def main():
    print("ВИЗУАЛИЗАЦИЯ: Демография + категории")
    
    data = get_demographic_data()
    
    if not data:
        print("Нет данных для анализа")
        return
    
    # Анализируем структуру ключей
    age_category_revenue = []
    age_category_count = []
    
    for item in data:
        key = item['key']
        
        # AGE_CATEGORY_18-24_Beauty
        if key.startswith('AGE_CATEGORY_') and not key.startswith('AGE_CATEGORY_COUNT_'):
            parts = key.split('_')
            if len(parts) >= 4:
                age_group = parts[2]
                category = parts[3]
                age_category_revenue.append({
                    'age_group': age_group,
                    'category': category,
                    'amount': item['value']
                })
        
        # AGE_CATEGORY_COUNT_18-24_Beauty
        elif key.startswith('AGE_CATEGORY_COUNT_'):
            parts = key.split('_')
            if len(parts) >= 5:
                age_group = parts[3]
                category = parts[4]
                age_category_count.append({
                    'age_group': age_group,
                    'category': category,
                    'count': item['value']
                })
    
    print(f"\nАНАЛИЗ ДЕМОГРАФИИ И КАТЕГОРИЙ")
    print("=" * 50)
    print(f"Возраст + Категории (выручка): {len(age_category_revenue)} комбинаций")
    print(f"Возраст + Категории (количество): {len(age_category_count)} комбинаций")
    
    # Создаем графики
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('ВОЗРАСТ × КАТЕГОРИИ: Демографический анализ', 
                fontsize=16, fontweight='bold')
    
    # 1. Heatmap: Возраст × Категория (Выручка)
    if age_category_revenue:
        # Создаем матрицу возраст × категория
        age_groups = sorted(list(set(item['age_group'] for item in age_category_revenue)))
        categories = sorted(list(set(item['category'] for item in age_category_revenue)))
        
        matrix_revenue = np.zeros((len(age_groups), len(categories)))
        
        for item in age_category_revenue:
            i = age_groups.index(item['age_group'])
            j = categories.index(item['category'])
            matrix_revenue[i][j] = item['amount']
        
        im1 = ax1.imshow(matrix_revenue, cmap='YlOrRd', aspect='auto')
        ax1.set_xticks(range(len(categories)))
        ax1.set_yticks(range(len(age_groups)))
        ax1.set_xticklabels(categories)
        ax1.set_yticklabels(age_groups)
        ax1.set_title('ВОЗРАСТ × КАТЕГОРИЯ\nHeatmap выручки ($)')
        ax1.set_xlabel('Категории')
        ax1.set_ylabel('Возрастные группы')
        
        for i in range(len(age_groups)):
            for j in range(len(categories)):
                if matrix_revenue[i][j] > 0:
                    ax1.text(j, i, f'${matrix_revenue[i][j]/1000:.0f}K', 
                            ha="center", va="center", color="black", fontsize=9,
                            fontweight='bold')
        
        plt.colorbar(im1, ax=ax1, label='Выручка ($)')
    
    # 2. Heatmap: Возраст × Категория (Количество транзакций)
    if age_category_count:
        # Создаем матрицу возраст × категория
        age_groups = sorted(list(set(item['age_group'] for item in age_category_count)))
        categories = sorted(list(set(item['category'] for item in age_category_count)))
        
        matrix_count = np.zeros((len(age_groups), len(categories)))
        
        for item in age_category_count:
            i = age_groups.index(item['age_group'])
            j = categories.index(item['category'])
            matrix_count[i][j] = item['count']
        
        im2 = ax2.imshow(matrix_count, cmap='Blues', aspect='auto')
        ax2.set_xticks(range(len(categories)))
        ax2.set_yticks(range(len(age_groups)))
        ax2.set_xticklabels(categories)
        ax2.set_yticklabels(age_groups)
        ax2.set_title('ВОЗРАСТ × КАТЕГОРИЯ\nHeatmap количества транзакций')
        ax2.set_xlabel('Категории')
        ax2.set_ylabel('Возрастные группы')
        
        for i in range(len(age_groups)):
            for j in range(len(categories)):
                if matrix_count[i][j] > 0:
                    ax2.text(j, i, f'{matrix_count[i][j]:.0f}', 
                            ha="center", va="center", color="black", fontsize=9,
                            fontweight='bold')
        
        plt.colorbar(im2, ax=ax2, label='Количество транзакций')
    
    # 3. Stacked bar: Выручка по возрастным группам
    if age_category_revenue:
        # Группируем по возрастным группам
        age_totals = {}
        for item in age_category_revenue:
            age_group = item['age_group']
            if age_group not in age_totals:
                age_totals[age_group] = {}
            age_totals[age_group][item['category']] = item['amount']
        
        age_groups = sorted(age_totals.keys())
        categories = sorted(list(set(item['category'] for item in age_category_revenue)))
        
        x = range(len(age_groups))
        bottom = np.zeros(len(age_groups))
        
        for i, category in enumerate(categories):
            values = [age_totals[age].get(category, 0) for age in age_groups]
            ax3.bar(x, values, bottom=bottom, label=category,
                   color=plt.cm.Set3(i / len(categories)))
            bottom += values
        
        ax3.set_xticks(x)
        ax3.set_xticklabels(age_groups)
        ax3.set_title('ВЫРУЧКА ПО ВОЗРАСТНЫМ ГРУППАМ\nStacked по категориям')
        ax3.set_ylabel('Выручка ($)')
        ax3.legend(title='Категории')
        
        # Добавляем общие суммы
        for i, age_group in enumerate(age_groups):
            total = sum(age_totals[age_group].values())
            ax3.text(i, total + 1000, f'${total/1000:.0f}K', 
                    ha='center', va='bottom', fontweight='bold')
    
    # 4. Топ комбинации Возраст × Категория
    if age_category_revenue:
        # Берем топ-10 по выручке
        top_10 = sorted(age_category_revenue, key=lambda x: x['amount'], reverse=True)[:10]
        
        labels = [f"{item['age_group']}\n{item['category']}" for item in top_10]
        values = [item['amount'] for item in top_10]
        
        bars = ax4.barh(range(len(labels)), values, color='lightgreen')
        ax4.set_yticks(range(len(labels)))
        ax4.set_yticklabels(labels)
        ax4.set_title('ТОП-10: ВОЗРАСТ × КАТЕГОРИЯ\nСамые прибыльные комбинации')
        ax4.set_xlabel('Выручка ($)')
        
        for i, (bar, value) in enumerate(zip(bars, values)):
            ax4.text(value + max(values)*0.01, i, f'${value/1000:.0f}K', 
                    va='center', fontsize=8, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('/scripts/demographic_category_analysis.png', dpi=100, bbox_inches='tight')
    plt.close()
    
    print("График сохранен: demographic_category_analysis.png")
    
    # Ключевые инсайты
    if age_category_revenue:
        top_combo = max(age_category_revenue, key=lambda x: x['amount'])
        print(f"\nСАМАЯ ПРИБЫЛЬНАЯ КОМБИНАЦИЯ:")
        print(f"  Возраст {top_combo['age_group']} покупают {top_combo['category']}")
        print(f"  Выручка: ${top_combo['amount']:,.2f}")
        
        # Анализ по возрастным группам
        print(f"\nВЫРУЧКА ПО ВОЗРАСТНЫМ ГРУППАМ:")
        age_totals = {}
        for item in age_category_revenue:
            age_group = item['age_group']
            if age_group not in age_totals:
                age_totals[age_group] = 0
            age_totals[age_group] += item['amount']
        
        for age_group in sorted(age_totals.keys()):
            total = age_totals[age_group]
            percentage = (total / sum(age_totals.values())) * 100
            print(f"  {age_group}: ${total:,.2f} ({percentage:.1f}%)")

if __name__ == '__main__':
    main()
```
```bash
docker cp demographic_category_analysis.py namenode:/scripts/
docker cp visualize_demographic_category.py namenode:/scripts/

python3 demographic_category_analysis.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/demographic_category
python3 visualize_demographic_category.py

docker cp namenode:/scripts/demographic_category_analysis.png ./
feh demographic_category_analysis.png
```
</details>

### **6. `time_pattern_analysis.py` - ВРЕМЕННЫЕ ПАТТЕРНЫ**
```python
"""
 ТЕОРЕТИЧЕСКАЯ ОСНОВА: Продажи по дням/месяцам + временные паттерны

ПРОБЛЕМА: Временные данные без анализа паттернов

РЕШЕНИЕ: Многоуровневый временной анализ:
"WEEKDAY_Monday" → $45,200
"WEEKEND_1" → $120,500 (выходные)
"CATEGORY_SEASON_Electronics_SUMMER" → $89,000

 ТЕХНИКА:
- Различные временные срезы: дни, недели, месяцы, сезоны
- Анализ будни/выходные
- Временные паттерны по категориям
"""
```
**Создаем `time_pattern_analysis.py`:**
```python
#!/usr/bin/env python3
from mrjob.job import MRJob
from datetime import datetime

class TimePatternAnalysis(MRJob):

    def mapper(self, _, line):
        if 'Transaction ID' in line:
            return
            
        parts = line.split(',')
        if len(parts) >= 9:
            try:
                date_str = parts[1].strip()
                category = parts[5].strip()
                total_amount = float(parts[8])
                
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                
                # ВРЕМЕННЫЕ ПАТТЕРНЫ
                year = date_obj.year
                month = date_obj.month
                day = date_obj.day
                weekday = date_obj.strftime('%A')  # Monday, Tuesday...
                week_of_month = (day - 1) // 7 + 1
                is_weekend = 1 if weekday in ['Saturday', 'Sunday'] else 0
                
                # Группировка по разным временным интервалам
                yield f"YEAR_{year}", total_amount
                yield f"YEAR_MONTH_{year}_{month:02d}", total_amount
                yield f"MONTH_{month:02d}", total_amount
                yield f"DAY_{day:02d}", total_amount
                yield f"WEEKDAY_{weekday}", total_amount
                yield f"WEEK_OF_MONTH_{week_of_month}", total_amount
                yield f"WEEKEND_{is_weekend}", total_amount
                
                # Временные паттерны по категориям
                yield f"CATEGORY_MONTH_{category}_{month:02d}", total_amount
                yield f"CATEGORY_WEEKDAY_{category}_{weekday}", total_amount
                yield f"CATEGORY_WEEKEND_{category}_{is_weekend}", total_amount
                
                # Сезонность по категориям
                season = self.get_season(month)
                yield f"CATEGORY_SEASON_{category}_{season}", total_amount
                
            except (ValueError, IndexError) as e:
                self.increment_counter('errors', 'parsing_error', 1)

    def get_season(self, month):
        if month in [12, 1, 2]: return "WINTER"
        elif month in [3, 4, 5]: return "SPRING"
        elif month in [6, 7, 8]: return "SUMMER"
        else: return "AUTUMN"

    def reducer(self, key, values):
        total = sum(values)
        count = sum(1 for _ in values)
        avg = total / count if count > 0 else 0
        
        if "WEEKDAY" in key or "DAY" in key:
            yield key, f"${total:,.2f} (ср. ${avg:.2f}, {count} транзакций)"
        elif "WEEKEND" in key:
            day_type = "выходные" if "1" in key else "будни"
            yield f"ПРОДАЖИ_{day_type}", f"${total:,.2f} (ср. ${avg:.2f})"
        else:
            yield key, f"${total:,.2f}"

if __name__ == '__main__':
    TimePatternAnalysis.run()
```
<details>
  <summary>Визуализация</summary>
    
**6. `visualize_time_patterns.py` - ВРЕМЕННЫЕ ПАТТЕРНЫ**
```python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import subprocess
import re
import numpy as np

def get_time_patterns_results():
    cmd = "hdfs dfs -cat /user/root/output/time_patterns/part-*"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    data = {}
    for line in result.stdout.strip().split('\n'):
        if '\t' in line:
            parts = line.split('\t')
            if len(parts) >= 2:
                key = parts[0].strip().strip('"')
                value_str = parts[1].strip().strip('"')
                
                # Используем регулярное выражение для извлечения числовой части
                match = re.search(r'(\d{1,3}(?:,\d{3})*\.\d{2})', value_str)
                if match:
                    numeric_str = match.group(1).replace(',', '')
                    try:
                        value = float(numeric_str)
                        data[key] = value
                    except ValueError:
                        data[key] = value_str
                else:
                    data[key] = value_str
    
    return data

def visualize_time_patterns():
    print("ВИЗУАЛИЗАЦИЯ: Временные паттерны")
    print("Анализ продаж по месяцам, категориям и временным периодам")
    
    results = get_time_patterns_results()
    print(f"Всего записей: {len(results)}")
    
    # Фильтруем только числовые значения
    numeric_results = {k: v for k, v in results.items() if isinstance(v, (int, float))}
    print(f"Числовых значений для анализа: {len(numeric_results)}")
    
    if not numeric_results:
        print("Нет числовых данных для анализа")
        return
    
    # Создаем графики
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('КОМПЛЕКСНЫЙ АНАЛИЗ ВРЕМЕННЫХ ПАТТЕРНОВ ПРОДАЖ', 
                fontsize=16, fontweight='bold', y=0.98)
    
    # 1. Общие продажи по месяцам
    monthly_data = {k: v for k, v in numeric_results.items() if k.startswith('MONTH_')}
    if monthly_data:
        months = []
        revenues = []
        for key, value in monthly_data.items():
            try:
                month_num = int(key.replace('MONTH_', ''))
                months.append(month_num)
                revenues.append(value)
            except:
                continue
        
        if months:
            month_df = pd.DataFrame({'month': months, 'revenue': revenues})
            month_df = month_df.sort_values('month')
            
            month_names = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 
                          'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек']
            
            bars = ax1.bar([month_names[m-1] for m in month_df['month']], month_df['revenue'], 
                          color='lightblue', alpha=0.7, edgecolor='navy')
            ax1.set_title('ОБЩИЕ ПРОДАЖИ ПО МЕСЯЦАМ', fontweight='bold')
            ax1.set_ylabel('Выручка ($)', fontweight='bold')
            ax1.tick_params(axis='x', rotation=45)
            
            # Добавляем значения на столбцы
            for bar, value in zip(bars, month_df['revenue']):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + 1000,
                        f'${value:,.0f}', ha='center', va='bottom', fontsize=8)
            
            ax1.grid(True, alpha=0.3, axis='y')
    
    # 2. Продажи по категориям и месяцам
    category_month_data = {}
    for key, value in numeric_results.items():
        if 'CATEGORY_MONTH_' in key and isinstance(value, (int, float)):
            parts = key.split('_')
            if len(parts) >= 4:
                category = parts[2]
                month = parts[3]
                try:
                    month_num = int(month)
                    if category not in category_month_data:
                        category_month_data[category] = {}
                    category_month_data[category][month_num] = value
                except:
                    continue
    
    if category_month_data:
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']
        month_names = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 
                      'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек']
        
        for i, (category, monthly_data) in enumerate(category_month_data.items()):
            sorted_months = sorted(monthly_data.keys())
            revenues = [monthly_data[month] for month in sorted_months]
            ax2.plot([month_names[m-1] for m in sorted_months], revenues, 
                    marker='o', linewidth=2, label=category, color=colors[i % len(colors)])
        
        ax2.set_title('ПРОДАЖИ ПО КАТЕГОРИЯМ И МЕСЯЦАМ', fontweight='bold')
        ax2.set_ylabel('Выручка ($)', fontweight='bold')
        ax2.legend()
        ax2.tick_params(axis='x', rotation=45)
        ax2.grid(True, alpha=0.3)
    
    # 3. Продажи по сезонам
    season_data = {}
    for key, value in numeric_results.items():
        if 'SEASON_' in key and isinstance(value, (int, float)):
            parts = key.split('_')
            if len(parts) >= 4:
                category = parts[2]
                season = parts[3]
                if category not in season_data:
                    season_data[category] = {}
                season_data[category][season] = value
    
    if season_data:
        seasons_order = ['WINTER', 'SPRING', 'SUMMER', 'AUTUMN']
        seasons_ru = ['Зима', 'Весна', 'Лето', 'Осень']
        
        categories = list(season_data.keys())
        x_pos = np.arange(len(seasons_order))
        bar_width = 0.8 / len(categories)
        
        for i, category in enumerate(categories):
            values = [season_data[category].get(season, 0) for season in seasons_order]
            ax3.bar(x_pos + i * bar_width, values, bar_width, 
                   label=category, alpha=0.7)
        
        ax3.set_title('ПРОДАЖИ ПО СЕЗОНАМ', fontweight='bold')
        ax3.set_ylabel('Выручка ($)', fontweight='bold')
        ax3.set_xticks(x_pos + bar_width * (len(categories) - 1) / 2)
        ax3.set_xticklabels(seasons_ru)
        ax3.legend()
        ax3.grid(True, alpha=0.3, axis='y')
    
    # 4. АЛЬТЕРНАТИВА: Продажи по годам или дням недели
    # Проверяем доступные данные для 4-го графика
    year_data = {k: v for k, v in numeric_results.items() if k.startswith('YEAR_') and 'MONTH' not in k}
    weekday_data = {k: v for k, v in numeric_results.items() if 'WEEKDAY_' in k and 'CATEGORY' not in k}
    week_data = {k: v for k, v in numeric_results.items() if 'WEEK_OF_MONTH_' in k}
    
    if year_data and len(year_data) > 1:
        # График продаж по годам
        years = []
        revenues = []
        for key, value in year_data.items():
            year = key.replace('YEAR_', '')
            years.append(year)
            revenues.append(value)
        
        bars = ax4.bar(years, revenues, color='gold', alpha=0.7)
        ax4.set_title('ПРОДАЖИ ПО ГОДАМ', fontweight='bold')
        ax4.set_ylabel('Выручка ($)', fontweight='bold')
        
        for bar, value in zip(bars, revenues):
            ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1000,
                    f'${value:,.0f}', ha='center', va='bottom', fontsize=9)
        ax4.grid(True, alpha=0.3, axis='y')
        
    elif weekday_data:
        # График продаж по дням недели
        weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        weekday_ru = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
        
        weekdays = []
        revenues = []
        for day in weekday_order:
            key = f"WEEKDAY_{day}"
            if key in weekday_data:
                weekdays.append(day)
                revenues.append(weekday_data[key])
        
        if weekdays:
            bars = ax4.bar([weekday_ru[weekday_order.index(day)] for day in weekdays], 
                          revenues, color='lightcoral', alpha=0.7)
            ax4.set_title('ПРОДАЖИ ПО ДНЯМ НЕДЕЛИ', fontweight='bold')
            ax4.set_ylabel('Выручка ($)', fontweight='bold')
            ax4.tick_params(axis='x', rotation=45)
            
            for bar, value in zip(bars, revenues):
                ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1000,
                        f'${value:,.0f}', ha='center', va='bottom', fontsize=9)
            ax4.grid(True, alpha=0.3, axis='y')
    
    elif week_data:
        # График продаж по неделям месяца
        weeks = []
        revenues = []
        for key, value in week_data.items():
            try:
                week_num = int(key.replace('WEEK_OF_MONTH_', ''))
                weeks.append(week_num)
                revenues.append(value)
            except:
                continue
        
        if weeks:
            week_df = pd.DataFrame({'week': weeks, 'revenue': revenues})
            week_df = week_df.sort_values('week')
            
            bars = ax4.bar(week_df['week'], week_df['revenue'], color='lightgreen', alpha=0.7)
            ax4.set_title('ПРОДАЖИ ПО НЕДЕЛЯМ МЕСЯЦА', fontweight='bold')
            ax4.set_xlabel('Неделя месяца')
            ax4.set_ylabel('Выручка ($)', fontweight='bold')
            ax4.grid(True, alpha=0.3, axis='y')
            
            for i, value in enumerate(week_df['revenue']):
                ax4.text(week_df['week'].iloc[i], value + 1000, f'${value:,.0f}', 
                        ha='center', va='bottom', fontsize=9)
    
    else:
        # Если нет данных для 4-го графика, показываем информационное сообщение
        ax4.text(0.5, 0.5, 'Нет данных для отображения\n\nДоступные данные:\n' +
                f'- Годы: {len(year_data)}\n' +
                f'- Дни недели: {len(weekday_data)}\n' +
                f'- Недели месяца: {len(week_data)}', 
                ha='center', va='center', transform=ax4.transAxes, fontsize=12)
        ax4.set_title('ДАННЫЕ ДЛЯ ГРАФИКА ОТСУТСТВУЮТ', fontweight='bold')
        ax4.set_xticks([])
        ax4.set_yticks([])
    
    plt.tight_layout()
    plt.savefig('/scripts/time_patterns_analysis.png', dpi=120, bbox_inches='tight')
    plt.close()
    
    print(" График сохранен: time_patterns_analysis.png")
    
    # Детальный анализ
    print("\n" + "="*80)
    print("ДЕТАЛЬНЫЙ АНАЛИЗ ВРЕМЕННЫХ ПАТТЕРНОВ")
    print("="*80)
    
    # Анализ по месяцам
    if monthly_data:
        total_revenue = sum(monthly_data.values())
        best_month_key = max(monthly_data.items(), key=lambda x: x[1])[0]
        worst_month_key = min(monthly_data.items(), key=lambda x: x[1])[0]
        
        month_names_full = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                          'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
        
        try:
            best_month_num = int(best_month_key.replace('MONTH_', ''))
            worst_month_num = int(worst_month_key.replace('MONTH_', ''))
            
            print(f"\n ОБЩИЙ АНАЛИЗ ПО МЕСЯЦАМ:")
            print(f"   Общая выручка за год: ${total_revenue:,.2f}")
            print(f"   Лучший месяц: {month_names_full[best_month_num-1]} (${monthly_data[best_month_key]:,.2f})")
            print(f"   Худший месяц: {month_names_full[worst_month_num-1]} (${monthly_data[worst_month_key]:,.2f})")
        except:
            print(f"\n ОБЩИЙ АНАЛИЗ ПО МЕСЯЦАМ:")
            print(f"   Общая выручка за год: ${total_revenue:,.2f}")
    
    # Анализ по категориям
    if category_month_data:
        print(f"\n АНАЛИЗ ПО КАТЕГОРИЯМ:")
        for category, monthly_data in category_month_data.items():
            total = sum(monthly_data.values())
            avg = total / len(monthly_data) if monthly_data else 0
            if monthly_data:
                best_month_val = max(monthly_data.values())
                best_month_num = max(monthly_data.items(), key=lambda x: x[1])[0]
                
                print(f"   {category}:")
                print(f"     - Общая выручка: ${total:,.2f}")
                print(f"     - Средняя в месяц: ${avg:,.2f}")
                print(f"     - Лучший месяц: {month_names_full[best_month_num-1]} (${best_month_val:,.2f})")
    
    # Анализ по сезонам
    if season_data:
        print(f"\n  АНАЛИЗ ПО СЕЗОНАМ:")
        season_totals = {}
        for category, seasons in season_data.items():
            for season, revenue in seasons.items():
                if season not in season_totals:
                    season_totals[season] = 0
                season_totals[season] += revenue
        
        season_names = {'WINTER': 'Зима', 'SPRING': 'Весна', 'SUMMER': 'Лето', 'AUTUMN': 'Осень'}
        for season, total in season_totals.items():
            print(f"   {season_names.get(season, season)}: ${total:,.2f}")
    
    # Анализ по годам
    if year_data:
        print(f"\n АНАЛИЗ ПО ГОДАМ:")
        for year, revenue in year_data.items():
            year_num = year.replace('YEAR_', '')
            print(f"   {year_num} год: ${revenue:,.2f}")
    
    print(f"\n СВОДНАЯ СТАТИСТИКА:")
    print(f"   Всего проанализировано записей: {len(numeric_results)}")
    if category_month_data:
        print(f"   Категории товаров: {list(category_month_data.keys())}")
    print(f"   Месяцы с данными: {len(monthly_data)}")
    if season_data:
        print(f"   Сезоны с данными: {len(season_data)}")
    if year_data:
        print(f"   Годы с данными: {len(year_data)}")
    if weekday_data:
        print(f"   Дни недели с данными: {len(weekday_data)}")

if __name__ == '__main__':
    visualize_time_patterns()
```
```bash
docker cp time_pattern_analysis.py namenode:/scripts/
docker cp visualize_time_patterns.py namenode:/scripts/

python3 time_pattern_analysis.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/time_patterns
python3 visualize_time_patterns.py
docker cp namenode:/scripts/time_patterns_analysis.png ./
feh time_patterns_analysis.png
```
</details>

### **7. `revenue_dynamics.py` - ДИНАМИКА МЕТРИК**
```python
"""
ТЕОРЕТИЧЕСКАЯ ОСНОВА: Динамика среднего чека и выручки

ПРОБЛЕМА: Статические метрики без трендов

РЕШЕНИЕ: Динамический анализ:
"СРЕДНИЙ_ЧЕК_2023-01" → $85.20
"ВЫРУЧКА_GENDER_CATEGORY_Male_Electronics" → $45,000
"GROWTH_BASE_2023-02" → +15%

ТЕХНИКА:
- Вычисление среднего чека
- Динамика по месяцам
- Кросс-анализ демографии и категорий
- Подготовка данных для анализа роста
"""
```
**Создаем `revenue_dynamics.py`:**
```python
#!/usr/bin/env python3
from mrjob.job import MRJob
from datetime import datetime
import statistics

class RevenueDynamicsAnalysis(MRJob):

    def mapper(self, _, line):
        if 'Transaction ID' in line:
            return
            
        parts = line.split(',')
        if len(parts) >= 9:
            try:
                date_str = parts[1].strip()
                gender = parts[3].strip()
                category = parts[5].strip()
                total_amount = float(parts[8])
                
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                year_month = date_obj.strftime('%Y-%m')
                
                # ДИНАМИКА СРЕДНЕГО ЧЕКА И ВЫРУЧКИ
                
                # 1. Динамика среднего чека по месяцам
                yield f"AVG_RECEIPT_MONTHLY_{year_month}", total_amount
                yield f"AVG_RECEIPT_COUNT_{year_month}", 1
                
                # 2. Средний чек по полу и категориям
                yield f"AVG_RECEIPT_GENDER_{gender}", total_amount
                yield f"AVG_RECEIPT_GENDER_COUNT_{gender}", 1
                
                yield f"AVG_RECEIPT_CATEGORY_{category}", total_amount
                yield f"AVG_RECEIPT_CATEGORY_COUNT_{category}", 1
                
                yield f"AVG_RECEIPT_GENDER_CATEGORY_{gender}_{category}", total_amount
                yield f"AVG_RECEIPT_GENDER_CATEGORY_COUNT_{gender}_{category}", 1
                
                # 3. Выручка по месяцам
                yield f"REVENUE_MONTHLY_{year_month}", total_amount
                
                # 4. Выручка по полу и категориям
                yield f"REVENUE_GENDER_{gender}", total_amount
                yield f"REVENUE_CATEGORY_{category}", total_amount
                yield f"REVENUE_GENDER_CATEGORY_{gender}_{category}", total_amount
                
                # 5. Рост выручки (месяц к месяцу)
                yield f"GROWTH_BASE_{year_month}", total_amount
                
            except (ValueError, IndexError) as e:
                self.increment_counter('errors', 'parsing_error', 1)

    def reducer(self, key, values):
        values_list = list(values)
        
        if "COUNT" in key:
            count = sum(values_list)
            yield key, count
        elif "AVG_RECEIPT" in key and "COUNT" not in key:
            # Вычисляем средний чек
            total = sum(values_list)
            count_key = key.replace("AVG_RECEIPT", "AVG_RECEIPT_COUNT")
            yield f"СРЕДНИЙ_ЧЕК_{key.split('_')[-1]}", f"${total/len(values_list):.2f}"
        elif "REVENUE" in key:
            total = sum(values_list)
            yield f"ВЫРУЧКА_{'_'.join(key.split('_')[1:])}", f"${total:,.2f}"
        elif "GROWTH_BASE" in key:
            total = sum(values_list)
            yield key, total
        else:
            total = sum(values_list)
            yield key, f"${total:,.2f}"

if __name__ == '__main__':
    RevenueDynamicsAnalysis.run()
```
<details>
  <summary>Визуализация</summary>
    
### **7. `visualize_revenue_dynamics.py` - ДИНАМИКА ВЫРУЧКИ**
```python
#!/usr/bin/env python3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import subprocess
import re
import numpy as np

def get_revenue_dynamics_results():
    cmd = "hdfs dfs -cat /user/root/output/revenue_dynamics/part-*"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    data = {}
    for line in result.stdout.strip().split('\n'):
        if '\t' in line:
            parts = line.split('\t')
            if len(parts) >= 2:
                key = parts[0].strip().strip('"')
                value_str = parts[1].strip().strip('"')
                
                try:
                    if '$' in value_str:
                        clean_value = value_str.replace('$', '').replace(',', '').split(' ')[0]
                        value = float(clean_value)
                    else:
                        value = float(value_str)
                    data[key] = value
                except (ValueError, IndexError):
                    data[key] = value_str
    
    return data

def visualize_revenue_dynamics():
    print("ВИЗУАЛИЗАЦИЯ: Динамика выручки")
    print("Анализ среднего чека и количества транзакций")
    
    results = get_revenue_dynamics_results()
    print(f"Всего получено записей: {len(results)}")
    
    if not results:
        print("Нет данных для анализа")
        return
    
    # Группируем данные по типам
    avg_receipt_category = {}
    avg_receipt_count_category = {}
    avg_receipt_count_monthly = {}
    gender_category_count = {}
    monthly_revenue = {}
    category_revenue = {}
    gender_revenue = {}
    gender_transaction_count = {}
    
    # Выведем все ключи для отладки
    print("\nОТЛАДКА - Ключи с GENDER_CATEGORY_COUNT:")
    for key in results.keys():
        if 'GENDER_CATEGORY_COUNT' in key:
            print(f"  '{key}' -> {results[key]}")
    
    for key, value in results.items():
        decoded_key = key.encode().decode('unicode_escape')
        
        # Средний чек по категориям
        if 'СРЕДНИЙ_ЧЕК_' in decoded_key and not any(x in decoded_key for x in ['2023', '2024', 'Female', 'Male']):
            category = decoded_key.replace('СРЕДНИЙ_ЧЕК_', '')
            avg_receipt_category[category] = value
        
        # Количество транзакций по категориям
        elif 'AVG_RECEIPT_CATEGORY_COUNT_' in key:
            category = key.replace('AVG_RECEIPT_CATEGORY_COUNT_', '')
            avg_receipt_count_category[category] = value
        
        # Количество транзакций по месяцам
        elif 'AVG_RECEIPT_COUNT_' in key:
            month = key.replace('AVG_RECEIPT_COUNT_', '')
            avg_receipt_count_monthly[month] = value
        
        # Количество транзакций по полу и категориям - ИСПРАВЛЕННЫЙ ПАРСИНГ
        elif 'GENDER_CATEGORY_COUNT_' in key:
            # Пример ключа: 'AVG_RECEIPT_GENDER_CATEGORY_COUNT_Female_Beauty'
            # Разбиваем по подчеркиваниям
            parts = key.split('_')
            print(f"Отладка парсинга '{key}': parts = {parts}")
            
            # Ищем индексы Female/Male и категорий
            if 'Female' in parts:
                gender_idx = parts.index('Female')
                if gender_idx + 1 < len(parts):
                    gender = 'Female'
                    category = parts[gender_idx + 1]
                    gender_key = f"{gender}_{category}"
                    gender_category_count[gender_key] = value
                    print(f"  Найдено: {gender_key} = {value}")
            
            elif 'Male' in parts:
                gender_idx = parts.index('Male')
                if gender_idx + 1 < len(parts):
                    gender = 'Male'
                    category = parts[gender_idx + 1]
                    gender_key = f"{gender}_{category}"
                    gender_category_count[gender_key] = value
                    print(f"  Найдено: {gender_key} = {value}")
        
        # Общее количество транзакций по полу
        elif 'GENDER_COUNT_' in key and 'CATEGORY' not in key:
            gender = key.replace('AVG_RECEIPT_GENDER_COUNT_', '')
            gender_transaction_count[gender] = value
        
        # Выручка по месяцам
        elif 'ВЫРУЧКА_MONTHLY_' in decoded_key:
            month = decoded_key.replace('ВЫРУЧКА_MONTHLY_', '')
            monthly_revenue[month] = value
        
        # Выручка по категориям
        elif 'ВЫРУЧКА_CATEGORY_' in decoded_key and 'GENDER' not in decoded_key:
            category = decoded_key.replace('ВЫРУЧКА_CATEGORY_', '')
            category_revenue[category] = value
        
        # Выручка по полу
        elif 'ВЫРУЧКА_GENDER_' in decoded_key and 'CATEGORY' not in decoded_key:
            gender = decoded_key.replace('ВЫРУЧКА_GENDER_', '')
            gender_revenue[gender] = value
    
    print(f"\nСГРУППИРОВАННЫЕ ДАННЫЕ:")
    print(f"- Средний чек по категориям: {len(avg_receipt_category)}")
    print(f"- Количество транзакций по категориям: {len(avg_receipt_count_category)}")
    print(f"- Количество транзакций по месяцам: {len(avg_receipt_count_monthly)}")
    print(f"- Распределение транзакций по полу и категориям: {len(gender_category_count)}")
    print(f"- Общее количество транзакций по полу: {len(gender_transaction_count)}")
    print(f"- Выручка по месяцам: {len(monthly_revenue)}")
    print(f"- Выручка по категориям: {len(category_revenue)}")
    print(f"- Выручка по полу: {len(gender_revenue)}")
    
    # Создаем графики
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('АНАЛИЗ ДИНАМИКИ ВЫРУЧКИ И ТРАНЗАКЦИЙ', 
                fontsize=16, fontweight='bold', y=0.98)
    
    # 1. Средний чек по категориям
    if avg_receipt_category:
        categories = list(avg_receipt_category.keys())
        amounts = list(avg_receipt_category.values())
        
        bars = ax1.bar(categories, amounts, color=['#FF6B6B', '#4ECDC4', '#45B7D1'], alpha=0.7)
        ax1.set_title('СРЕДНИЙ ЧЕК ПО КАТЕГОРИЯМ', fontweight='bold')
        ax1.set_ylabel('Средний чек ($)', fontweight='bold')
        ax1.tick_params(axis='x', rotation=45)
        
        for bar, amount in zip(bars, amounts):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                    f'${amount:.2f}', ha='center', va='bottom', fontweight='bold')
        
        ax1.grid(True, alpha=0.3, axis='y')
    
    # 2. Выручка по месяцам
    if monthly_revenue:
        months = sorted(monthly_revenue.keys())
        revenues = [monthly_revenue[m] for m in months]
        
        month_labels = []
        for month in months:
            if '-' in month:
                year, month_num = month.split('-')
                month_names = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 
                              'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек']
                try:
                    month_labels.append(f"{month_names[int(month_num)-1]}\n{year}")
                except:
                    month_labels.append(month)
            else:
                month_labels.append(month)
        
        bars = ax2.bar(month_labels, revenues, color='lightgreen', alpha=0.7)
        ax2.set_title('ВЫРУЧКА ПО МЕСЯЦАМ', fontweight='bold')
        ax2.set_ylabel('Выручка ($)', fontweight='bold')
        ax2.tick_params(axis='x', rotation=45)
        
        for bar, revenue in zip(bars, revenues):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1000,
                    f'${revenue:,.0f}', ha='center', va='bottom', fontsize=8)
        
        ax2.grid(True, alpha=0.3, axis='y')
    
    # 3. Выручка по категориям
    if category_revenue:
        categories = list(category_revenue.keys())
        revenues = list(category_revenue.values())
        
        colors = ['#FF9999', '#99FF99', '#9999FF']
        ax3.pie(revenues, labels=categories, autopct='%1.1f%%', colors=colors,
               startangle=90, textprops={'fontweight': 'bold'})
        ax3.set_title('РАСПРЕДЕЛЕНИЕ ВЫРУЧКИ\nПО КАТЕГОРИЯМ', fontweight='bold')
    
    # 4. Транзакции по полу и категориям
    if gender_category_count:
        print(f"\nДАННЫЕ ДЛЯ ГРАФИКА ТРАНЗАКЦИЙ:")
        for key, value in gender_category_count.items():
            print(f"  {key}: {value}")
        
        # Группируем данные для графика
        categories = ['Beauty', 'Clothing', 'Electronics']
        female_counts = []
        male_counts = []
        
        for category in categories:
            female_counts.append(gender_category_count.get(f'Female_{category}', 0))
            male_counts.append(gender_category_count.get(f'Male_{category}', 0))
        
        x_pos = np.arange(len(categories))
        bar_width = 0.35
        
        bars1 = ax4.bar(x_pos - bar_width/2, female_counts, bar_width, 
                       label='Женщины', color='pink', alpha=0.7)
        bars2 = ax4.bar(x_pos + bar_width/2, male_counts, bar_width, 
                       label='Мужчины', color='lightblue', alpha=0.7)
        
        ax4.set_title('ТРАНЗАКЦИИ ПО ПОЛУ И КАТЕГОРИЯМ', fontweight='bold')
        ax4.set_ylabel('Количество транзакций', fontweight='bold')
        ax4.set_xlabel('Категории', fontweight='bold')
        ax4.set_xticks(x_pos)
        ax4.set_xticklabels(categories)
        ax4.legend()
        ax4.grid(True, alpha=0.3, axis='y')
        
        # Добавляем значения на столбцы
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax4.text(bar.get_x() + bar.get_width()/2, height + 2,
                            f'{int(height)}', ha='center', va='bottom', fontsize=9)
    else:
        # Альтернатива: общее количество транзакций по полу
        if gender_transaction_count:
            genders = list(gender_transaction_count.keys())
            counts = list(gender_transaction_count.values())
            gender_labels = {'Female': 'Женщины', 'Male': 'Мужчины'}
            labels = [gender_labels.get(g, g) for g in genders]
            
            colors = ['pink', 'lightblue']
            bars = ax4.bar(labels, counts, color=colors, alpha=0.7)
            ax4.set_title('ОБЩЕЕ КОЛИЧЕСТВО ТРАНЗАКЦИЙ\nПО ПОЛУ', fontweight='bold')
            ax4.set_ylabel('Количество транзакций', fontweight='bold')
            
            for bar, count in zip(bars, counts):
                ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                        f'{int(count)}', ha='center', va='bottom', fontweight='bold')
            
            ax4.grid(True, alpha=0.3, axis='y')
        else:
            ax4.text(0.5, 0.5, 'Нет данных для графика', 
                    ha='center', va='center', transform=ax4.transAxes, fontsize=12)
            ax4.set_title('ТРАНЗАКЦИИ ПО ПОЛУ', fontweight='bold')
            ax4.set_xticks([])
            ax4.set_yticks([])
    
    plt.tight_layout()
    plt.savefig('/scripts/revenue_dynamics_analysis.png', dpi=120, bbox_inches='tight')
    plt.close()
    
    print("\nГрафик сохранен: revenue_dynamics_analysis.png")
    
    # Детальный анализ
    print("\n" + "="*80)
    print("ДЕТАЛЬНЫЙ АНАЛИЗ ДИНАМИКИ ВЫРУЧКИ")
    print("="*80)
    
    if avg_receipt_category:
        print(f"\nСРЕДНИЙ ЧЕК ПО КАТЕГОРИЯМ:")
        for category, amount in avg_receipt_category.items():
            print(f"   {category}: ${amount:.2f}")
        avg_all = sum(avg_receipt_category.values()) / len(avg_receipt_category)
        print(f"   Средний чек по всем категориям: ${avg_all:.2f}")
    
    if monthly_revenue:
        print(f"\nВЫРУЧКА ПО МЕСЯЦАМ:")
        total_revenue = sum(monthly_revenue.values())
        best_month = max(monthly_revenue.items(), key=lambda x: x[1])
        worst_month = min(monthly_revenue.items(), key=lambda x: x[1])
        print(f"   Общая выручка: ${total_revenue:,.2f}")
        print(f"   Лучший месяц: {best_month[0]} (${best_month[1]:,.2f})")
        print(f"   Худший месяц: {worst_month[0]} (${worst_month[1]:,.2f})")
        print(f"   Средняя месячная выручка: ${total_revenue/len(monthly_revenue):,.2f}")
    
    if category_revenue:
        print(f"\nВЫРУЧКА ПО КАТЕГОРИЯМ:")
        total_revenue = sum(category_revenue.values())
        for category, revenue in category_revenue.items():
            percentage = (revenue / total_revenue) * 100
            print(f"   {category}: ${revenue:,.2f} ({percentage:.1f}%)")
    
    if gender_revenue:
        print(f"\nВЫРУЧКА ПО ПОЛУ:")
        total_revenue = sum(gender_revenue.values())
        for gender, revenue in gender_revenue.items():
            percentage = (revenue / total_revenue) * 100
            gender_name = 'Женщины' if gender == 'Female' else 'Мужчины' if gender == 'Male' else gender
            print(f"   {gender_name}: ${revenue:,.2f} ({percentage:.1f}%)")
    
    if avg_receipt_count_category:
        print(f"\nКОЛИЧЕСТВО ТРАНЗАКЦИЙ ПО КАТЕГОРИЯМ:")
        total_transactions = sum(avg_receipt_count_category.values())
        for category, count in avg_receipt_count_category.items():
            percentage = (count / total_transactions) * 100
            print(f"   {category}: {int(count)} транзакций ({percentage:.1f}%)")
        print(f"   Всего транзакций: {int(total_transactions)}")
    
    if gender_transaction_count:
        print(f"\nОБЩЕЕ КОЛИЧЕСТВО ТРАНЗАКЦИЙ ПО ПОЛУ:")
        total_transactions = sum(gender_transaction_count.values())
        for gender, count in gender_transaction_count.items():
            percentage = (count / total_transactions) * 100
            gender_name = 'Женщины' if gender == 'Female' else 'Мужчины' if gender == 'Male' else gender
            print(f"   {gender_name}: {int(count)} транзакций ({percentage:.1f}%)")
    
    if gender_category_count:
        print(f"\nТРАНЗАКЦИИ ПО ПОЛУ И КАТЕГОРИЯМ:")
        categories = ['Beauty', 'Clothing', 'Electronics']
        for category in categories:
            female_count = gender_category_count.get(f'Female_{category}', 0)
            male_count = gender_category_count.get(f'Male_{category}', 0)
            print(f"   {category}:")
            print(f"     - Женщины: {int(female_count)}")
            print(f"     - Мужчины: {int(male_count)}")
    else:
        print(f"\nТРАНЗАКЦИИ ПО ПОЛУ И КАТЕГОРИЯМ: данные не найдены")

if __name__ == '__main__':
    visualize_revenue_dynamics()
```


```bash
docker cp revenue_dynamics.py namenode:/scripts/
docker cp visualize_revenue_dynamics.py namenode:/scripts/

python3 revenue_dynamics.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/revenue_dynamics

python3 visualize_revenue_dynamics.py
docker cp namenode:/scripts/revenue_dynamics_analysis.png ./
feh revenue_dynamics_analysis.png
```
</details>


### **8. `comprehensive_time_analysis.py` - КОМПЛЕКСНЫЙ АНАЛИЗ**
```python
"""
ТЕОРЕТИЧЕСКАЯ ОСНОВА: Полные пересечения измерений

ПРОБЛЕМА: Простые анализа не показывают полную картину

РЕШЕНИЕ: Комплексные паттерны:
"FULL_PATTERN_Male_25-34_Electronics_MORNING" → $12,500
"FULL_PATTERN_Female_35-44_Clothing_Saturday" → $8,200

ТЕХНИКА:
- Полные пересечения: Демография + Категория + Время
- Анализ времени суток: MORNING/AFTERNOON/EVENING/NIGHT
- Дни недели + время суток
- Максимальная детализация паттернов покупок
"""
```
**Создаем `comprehensive_time_analysis.py`:**
```python
#!/usr/bin/env python3
from mrjob.job import MRJob
from datetime import datetime

class ComprehensiveTimeAnalysis(MRJob):

    def mapper(self, _, line):
        if 'Transaction ID' in line:
            return
            
        parts = line.split(',')
        if len(parts) >= 9:
            try:
                date_str = parts[1].strip()
                gender = parts[3].strip()
                age = int(parts[4])
                category = parts[5].strip()
                total_amount = float(parts[8])
                
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                
                # КОМПЛЕКСНЫЕ ВРЕМЕННЫЕ ПАТТЕРНЫ
                year_month = date_obj.strftime('%Y-%m')
                day = date_obj.day
                weekday = date_obj.strftime('%A')
                hour = date_obj.hour if date_obj.hour else 12  # если время не указано
                
                age_group = self.get_age_group(age)
                time_of_day = self.get_time_of_day(hour)
                
                # 1. Продажи по дням месяца
                yield f"DAY_OF_MONTH_{day:02d}", total_amount
                
                # 2. Временные паттерны по категориям
                yield f"CATEGORY_DAY_{category}_{day:02d}", total_amount
                yield f"CATEGORY_WEEKDAY_{category}_{weekday}", total_amount
                yield f"CATEGORY_TIME_{category}_{time_of_day}", total_amount
                
                # 3. Демография + время
                yield f"GENDER_TIME_{gender}_{time_of_day}", total_amount
                yield f"AGE_TIME_{age_group}_{time_of_day}", total_amount
                
                # 4. Полные пересечения: Демография + Категория + Время
                yield f"FULL_PATTERN_{gender}_{age_group}_{category}_{time_of_day}", total_amount
                yield f"FULL_PATTERN_{gender}_{age_group}_{category}_{weekday}", total_amount
                
            except (ValueError, IndexError) as e:
                self.increment_counter('errors', 'parsing_error', 1)

    def get_age_group(self, age):
        if age <= 24: return "18-24"
        elif age <= 34: return "25-34"
        elif age <= 44: return "35-44"
        elif age <= 54: return "45-54"
        else: return "55+"

    def get_time_of_day(self, hour):
        if 5 <= hour < 12: return "MORNING"
        elif 12 <= hour < 17: return "AFTERNOON"
        elif 17 <= hour < 22: return "EVENING"
        else: return "NIGHT"

    def reducer(self, key, values):
        total = sum(values)
        count = sum(1 for _ in values)
        
        if "DAY_OF_MONTH" in key:
            day = key.split('_')[-1]
            yield f" День {day}", f"${total:,.2f} ({count} заказов)"
        elif "FULL_PATTERN" in key:
            yield f" {key.replace('FULL_PATTERN_', '')}", f"${total:,.2f}"
        elif "CATEGORY_TIME" in key or "GENDER_TIME" in key:
            parts = key.split('_')
            yield f" {parts[-2]} {parts[-1]}", f"${total:,.2f}"
        else:
            yield key, f"${total:,.2f}"

if __name__ == '__main__':
    ComprehensiveTimeAnalysis.run()
```

<details>
  <summary>Визуализация</summary>

**8. `visualize_comprehensive_time.py` - КОМПЛЕКСНЫЙ ВРЕМЕННОЙ АНАЛИЗ**
```python
#!/usr/bin/env python3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import subprocess
import re
import numpy as np

def get_comprehensive_time_results():
    cmd = "hdfs dfs -cat /user/root/output/comprehensive_time/part-*"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    data = {}
    for line in result.stdout.strip().split('\n'):
        if '\t' in line:
            parts = line.split('\t')
            if len(parts) >= 2:
                key = parts[0].strip().strip('"')
                value_str = parts[1].strip().strip('"')
                
                try:
                    if '$' in value_str:
                        clean_value = value_str.replace('$', '').replace(',', '').split(' ')[0]
                        value = float(clean_value)
                    else:
                        value = float(value_str)
                    data[key] = value
                except (ValueError, IndexError):
                    data[key] = value_str
    
    return data

def visualize_comprehensive_time():
    print("ВИЗУАЛИЗАЦИЯ: Комплексный временной анализ")
    print("Анализ продаж по времени суток, возрастам и категориям")
    
    results = get_comprehensive_time_results()
    print(f"Всего получено записей: {len(results)}")
    
    if not results:
        print("Нет данных для анализа")
        return
    
    # Группируем данные по всем типам с правильной обработкой пробелов
    age_time_data = {}           # AGE_TIME_
    category_day_data = {}       # CATEGORY_DAY_
    gender_age_category_data = {} #  Female_,  Male_ (с пробелом!)
    
    # Специальная обработка для ключей с пробелами
    female_data = {}
    male_data = {}
    
    for key, value in results.items():
        # Продажи по возрастам и времени суток
        if key.startswith('AGE_TIME_'):
            parts = key.replace('AGE_TIME_', '').split('_')
            if len(parts) >= 2:
                age_group = parts[0]
                time_of_day = parts[1]
                age_time_data[f"{age_group}_{time_of_day}"] = value
        
        # Продажи по категориям и дням месяца
        elif key.startswith('CATEGORY_DAY_'):
            parts = key.replace('CATEGORY_DAY_', '').split('_')
            if len(parts) >= 2:
                category = parts[0]
                day = parts[1]
                category_day_data[f"{category}_{day}"] = value
        
        # Обработка ключей с пробелами в начале
        elif key.startswith(' Female_'):
            # Пример: ' Female_18-24_Beauty_AFTERNOON'
            parts = key.split('_')
            if len(parts) >= 4:
                # Убираем пробел из первого элемента
                gender = parts[0].strip()  # 'Female'
                age_group = parts[1]       # '18-24'
                category = parts[2]        # 'Beauty'
                time_period = parts[3]     # 'AFTERNOON' или 'Friday'
                
                # Сохраняем в разных форматах
                key_full = f"{gender}_{age_group}_{category}_{time_period}"
                gender_age_category_data[key_full] = value
                female_data[key_full] = value
        
        elif key.startswith(' Male_'):
            # Пример: ' Male_18-24_Beauty_AFTERNOON'
            parts = key.split('_')
            if len(parts) >= 4:
                # Убираем пробел из первого элемента
                gender = parts[0].strip()  # 'Male'
                age_group = parts[1]       # '18-24'
                category = parts[2]        # 'Beauty'
                time_period = parts[3]     # 'AFTERNOON' или 'Friday'
                
                # Сохраняем в разных форматах
                key_full = f"{gender}_{age_group}_{category}_{time_period}"
                gender_age_category_data[key_full] = value
                male_data[key_full] = value
    
    print(f"\nСГРУППИРОВАННЫЕ ДАННЫЕ:")
    print(f"- Возраст + время: {len(age_time_data)}")
    print(f"- Категория + день: {len(category_day_data)}")
    print(f"- Полные паттерны (Female): {len(female_data)}")
    print(f"- Полные паттерны (Male): {len(male_data)}")
    print(f"- Все полные паттерны: {len(gender_age_category_data)}")
    
    # Выведем несколько примеров полных паттернов для отладки
    if gender_age_category_data:
        print(f"\nПримеры полных паттернов:")
        for i, (key, value) in enumerate(list(gender_age_category_data.items())[:5]):
            print(f"  {i+1}. {key} -> ${value:,.2f}")
    
    # Создаем графики
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('КОМПЛЕКСНЫЙ ВРЕМЕННОЙ АНАЛИЗ ПРОДАЖ', 
                fontsize=16, fontweight='bold', y=0.98)
    
    # 1. Продажи по возрастам и времени суток (тепловая карта)
    if age_time_data:
        age_groups = sorted(list(set([k.split('_')[0] for k in age_time_data.keys()])))
        time_periods = sorted(list(set([k.split('_')[1] for k in age_time_data.keys()])))
        
        # Создаем матрицу данных
        age_time_matrix = np.zeros((len(age_groups), len(time_periods)))
        
        for i, age in enumerate(age_groups):
            for j, time in enumerate(time_periods):
                key = f"{age}_{time}"
                age_time_matrix[i, j] = age_time_data.get(key, 0)
        
        # Строим тепловую карту
        im = ax1.imshow(age_time_matrix, cmap='YlOrRd', aspect='auto')
        ax1.set_title('ПРОДАЖИ: ВОЗРАСТ И ВРЕМЯ СУТОК', fontweight='bold')
        ax1.set_xlabel('Время суток', fontweight='bold')
        ax1.set_ylabel('Возрастные группы', fontweight='bold')
        ax1.set_xticks(range(len(time_periods)))
        ax1.set_xticklabels(time_periods, rotation=45)
        ax1.set_yticks(range(len(age_groups)))
        ax1.set_yticklabels(age_groups)
        
        # Добавляем значения в ячейки
        for i in range(len(age_groups)):
            for j in range(len(time_periods)):
                value = age_time_matrix[i, j]
                if value > 0:
                    ax1.text(j, i, f'${value/1000:.0f}K', 
                            ha='center', va='center', fontweight='bold', fontsize=8)
        
        plt.colorbar(im, ax=ax1, label='Выручка ($)')
    else:
        ax1.text(0.5, 0.5, 'Нет данных по возрастам и времени', 
                ha='center', va='center', transform=ax1.transAxes, fontsize=12)
        ax1.set_title('ПРОДАЖИ: ВОЗРАСТ И ВРЕМЯ СУТОК', fontweight='bold')
        ax1.set_xticks([])
        ax1.set_yticks([])
    
    # 2. Продажи по категориям и дням месяца
    if category_day_data:
        # Группируем по категориям
        categories = sorted(list(set([k.split('_')[0] for k in category_day_data.keys()])))
        days = sorted(list(set([int(k.split('_')[1]) for k in category_day_data.keys()])))
        
        # Собираем данные для графика
        category_revenues = {}
        for category in categories:
            revenues = []
            for day in days:
                key = f"{category}_{day:02d}" if day < 10 else f"{category}_{day}"
                revenues.append(category_day_data.get(key, 0))
            category_revenues[category] = revenues
        
        # Строим линейные графики для всех категорий
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
        for i, (category, revenues) in enumerate(category_revenues.items()):
            if i < len(colors):  # Ограничиваем количество цветов
                ax2.plot(days, revenues, marker='o', linewidth=2, 
                        label=category, color=colors[i], markersize=3, alpha=0.7)
        
        ax2.set_title('ДИНАМИКА ПРОДАЖ ПО КАТЕГОРИЯМ\nИ ДНЯМ МЕСЯЦА', fontweight='bold')
        ax2.set_xlabel('День месяца', fontweight='bold')
        ax2.set_ylabel('Выручка ($)', fontweight='bold')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.set_xlim(min(days), max(days))
    else:
        ax2.text(0.5, 0.5, 'Нет данных по категориям и дням', 
                ha='center', va='center', transform=ax2.transAxes, fontsize=12)
        ax2.set_title('ДИНАМИКА ПРОДАЖ ПО КАТЕГОРИЯМ\nИ ДНЯМ МЕСЯЦА', fontweight='bold')
        ax2.set_xticks([])
        ax2.set_yticks([])
    
    # 3. Сравнение продаж по полу (суммарно)
    if female_data or male_data:
        female_total = sum(female_data.values()) if female_data else 0
        male_total = sum(male_data.values()) if male_data else 0
        
        genders = ['Женщины', 'Мужчины']
        totals = [female_total, male_total]
        colors = ['pink', 'lightblue']
        
        bars = ax3.bar(genders, totals, color=colors, alpha=0.7)
        ax3.set_title('СРАВНЕНИЕ ПРОДАЖ ПО ПОЛУ', fontweight='bold')
        ax3.set_ylabel('Выручка ($)', fontweight='bold')
        
        for bar, total in zip(bars, totals):
            ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1000,
                    f'${total/1000:.1f}K', ha='center', va='bottom', fontweight='bold')
        
        ax3.grid(True, alpha=0.3, axis='y')
    else:
        ax3.text(0.5, 0.5, 'Нет данных по полу', 
                ha='center', va='center', transform=ax3.transAxes, fontsize=12)
        ax3.set_title('СРАВНЕНИЕ ПРОДАЖ ПО ПОЛУ', fontweight='bold')
        ax3.set_xticks([])
        ax3.set_yticks([])
    
    # 4. Топ полные паттерны (пол + возраст + категория + время)
    if gender_age_category_data:
        # Берем топ-8 самых прибыльных паттернов
        top_patterns = sorted(gender_age_category_data.items(), 
                             key=lambda x: x[1], reverse=True)[:8]
        
        # Форматируем названия паттернов для читаемости
        patterns = []
        revenues = []
        for pattern, revenue in top_patterns:
            parts = pattern.split('_')
            if len(parts) >= 4:
                gender_ru = 'М' if parts[0] == 'Male' else 'Ж'
                age = parts[1]
                category = parts[2]
                time = parts[3]
                
                # Создаем читаемое название
                pattern_name = f"{gender_ru}-{age}\n{category}-{time}"
                patterns.append(pattern_name)
                revenues.append(revenue)
        
        bars = ax4.barh(patterns, revenues, color='lightgreen', alpha=0.7)
        ax4.set_title('ТОП-8 ПОЛНЫХ ПАТТЕРНОВ\n(Пол+Возраст+Категория+Время)', fontweight='bold')
        ax4.set_xlabel('Выручка ($)', fontweight='bold')
        
        for bar, value in zip(bars, revenues):
            ax4.text(value + 1000, bar.get_y() + bar.get_height()/2, 
                    f'${value/1000:.1f}K', va='center', fontsize=8)
        
        # Настраиваем внешний вид
        ax4.grid(True, alpha=0.3, axis='x')
    else:
        # Альтернативный график - распределение по времени суток из age_time_data
        if age_time_data:
            # Группируем по времени суток
            time_totals = {}
            for key, value in age_time_data.items():
                time_of_day = key.split('_')[1]
                if time_of_day not in time_totals:
                    time_totals[time_of_day] = 0
                time_totals[time_of_day] += value
            
            times = list(time_totals.keys())
            revenues = list(time_totals.values())
            
            # Переводим на русский
            time_translation = {
                'MORNING': 'Утро', 'AFTERNOON': 'День', 
                'EVENING': 'Вечер', 'NIGHT': 'Ночь'
            }
            labels = [time_translation.get(t, t) for t in times]
            
            colors = ['#FFD700', '#87CEEB', '#FF69B4', '#4B0082']
            bars = ax4.bar(labels, revenues, color=colors, alpha=0.7)
            ax4.set_title('ПРОДАЖИ ПО ВРЕМЕНИ СУТОК', fontweight='bold')
            ax4.set_ylabel('Выручка ($)', fontweight='bold')
            
            for bar, revenue in zip(bars, revenues):
                ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1000,
                        f'${revenue/1000:.0f}K', ha='center', va='bottom', fontweight='bold')
            
            ax4.grid(True, alpha=0.3, axis='y')
        else:
            ax4.text(0.5, 0.5, 'Нет данных для графика', 
                    ha='center', va='center', transform=ax4.transAxes, fontsize=12)
            ax4.set_title('ДАННЫЕ ДЛЯ ГРАФИКА', fontweight='bold')
            ax4.set_xticks([])
            ax4.set_yticks([])
    
    plt.tight_layout()
    plt.savefig('/scripts/comprehensive_time_analysis.png', dpi=120, bbox_inches='tight')
    plt.close()
    
    print("\nГрафик сохранен: comprehensive_time_analysis.png")
    
    # Детальный анализ
    print("\n" + "="*80)
    print("ДЕТАЛЬНЫЙ АНАЛИЗ ВРЕМЕННЫХ ПАТТЕРНОВ")
    print("="*80)
    
    if age_time_data:
        print(f"\nАНАЛИЗ ПО ВОЗРАСТУ И ВРЕМЕНИ:")
        age_totals = {}
        time_totals = {}
        
        for key, value in age_time_data.items():
            age_group = key.split('_')[0]
            time_of_day = key.split('_')[1]
            
            if age_group not in age_totals:
                age_totals[age_group] = 0
            age_totals[age_group] += value
            
            if time_of_day not in time_totals:
                time_totals[time_of_day] = 0
            time_totals[time_of_day] += value
        
        total_all = sum(age_totals.values())
        
        print(f"  По возрастам:")
        for age, revenue in sorted(age_totals.items()):
            percentage = (revenue / total_all) * 100
            print(f"    {age}: ${revenue:,.2f} ({percentage:.1f}%)")
        
        print(f"  По времени суток:")
        time_translation = {'MORNING': 'Утро', 'AFTERNOON': 'День', 'EVENING': 'Вечер', 'NIGHT': 'Ночь'}
        for time, revenue in sorted(time_totals.items()):
            percentage = (revenue / total_all) * 100
            time_name = time_translation.get(time, time)
            print(f"    {time_name}: ${revenue:,.2f} ({percentage:.1f}%)")
    
    if category_day_data:
        print(f"\nАНАЛИЗ ПО КАТЕГОРИЯМ:")
        category_totals = {}
        for key, value in category_day_data.items():
            category = key.split('_')[0]
            if category not in category_totals:
                category_totals[category] = 0
            category_totals[category] += value
        
        total_categories = sum(category_totals.values())
        for category, total in sorted(category_totals.items()):
            percentage = (total / total_categories) * 100
            print(f"   {category}: ${total:,.2f} ({percentage:.1f}%)")
    
    if female_data or male_data:
        print(f"\nАНАЛИЗ ПО ПОЛУ:")
        female_total = sum(female_data.values()) if female_data else 0
        male_total = sum(male_data.values()) if male_data else 0
        total_gender = female_total + male_total
        
        if total_gender > 0:
            print(f"   Женщины: ${female_total:,.2f} ({(female_total/total_gender)*100:.1f}%)")
            print(f"   Мужчины: ${male_total:,.2f} ({(male_total/total_gender)*100:.1f}%)")

if __name__ == '__main__':
    visualize_comprehensive_time()
```
```bash
docker cp comprehensive_time_analysis.py namenode:/scripts/
docker cp visualize_comprehensive_time.py namenode:/scripts/

python3 comprehensive_time_analysis.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/comprehensive_time
python3 visualize_comprehensive_time.py

docker cp namenode:/scripts/comprehensive_time_analysis.png ./
feh comprehensive_time_analysis.png

```
</details>
---

## **📊 СВОДНАЯ ТАБЛИЦА СВЯЗИ ТЕОРИИ И ПРАКТИКИ**

| Скрипт | Теоретический паттерн | Бизнес-ценность |
|--------|---------------------|-----------------|
| `secondary_sort.py` | Вторичная сортировка | Упорядоченный анализ по времени и категориям |
| `composite_keys.py` | Составные ключи | Многомерный анализ в одном проходе |
| `multiple_outputs.py` | Multiple Outputs | Комплексная аналитика из одного Job |
| `real_price_elasticity.py` | Сложные метрики | Понимание ценообразования и спроса |
| `demographic_category.py` | Многомерная группировка | Демографические портреты покупателей |
| `time_pattern_analysis.py` | Временные паттерны | Сезонность и цикличность продаж |
| `revenue_dynamics.py` | Динамика метрик | Тренды и рост бизнеса |
| `comprehensive_time.py` | Полные пересечения | Детальные паттерны поведения |

---





