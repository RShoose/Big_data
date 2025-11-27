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

### **Решение: Составной ключ с группировкой**
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

### **Визуализация: Продажи по месяцам и категориям**
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

### **Решение: Единый проход с составными ключами**
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

### **Визуализация: Гендерное распределение по категориям**
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

### **Визуализация: Комплексная дашборд-аналитика**
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
# **Практика**
---

## **🎯 СКРИПТЫ И ИХ ТЕОРЕТИЧЕСКАЯ БАЗА**

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
Копируем исправленные скрипты в контейнер
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
    
### **1. `visualize_secondary_sort.py` - ВТОРИЧНАЯ СОРТИРОВКА**
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
📚 ТЕОРЕТИЧЕСКАЯ ОСНОВА: Multiple Outputs - единый проход для всей аналитики

ПРОБЛЕМА: Разные типы аналитики требуют разных форматов вывода

РЕШЕНИЕ: Единый Mapper → Multiple Outputs:
├── TREND_MONTHLY_2023-01 → $45,000
├── DEMO_GENDER_Male → $150,000  
├── PRODUCT_Electronics_REVENUE → $90,000
├── METRIC_AVG_RECEIPT → $85.20
└── SEGMENT_HIGH_VALUE_Male_25-34 → $45,000

🔧 ТЕХНИКА:
- Разные префиксы ключей = разные типы аналитики
- Единый проход по данным
- Раздельная обработка в reducer
"""
```

### **4. `real_price_elasticity.py` - ЦЕНОВАЯ ЭЛАСТИЧНОСТЬ**
```python
"""
📚 ТЕОРЕТИЧЕСКАЯ ОСНОВА: Сложные бизнес-метрики

ПРОБЛЕМА: Простой анализ не показывает зависимость спроса от цены

РЕШЕНИЕ: Анализ ценовой эластичности:
"ELASTICITY_Electronics_PRICE" → {"avg": 85.50, "min": 25, "max": 299}
"ELASTICITY_Electronics_QUANTITY" → "2.1 ед."
"SEGMENT_PRICE_Electronics_PREMIUM" → $45,200

🔧 ТЕХНИКА:
- Статистические агрегаты (mean, min, max)
- Сегментация: BUDGET/STANDARD/PREMIUM/LUXURY
- Анализ объемов: SINGLE/SMALL/MEDIUM/BULK
- Соотношение цена/количество
"""
```

### **5. `demographic_category_analysis.py` - МНОГОМЕРНАЯ ГРУППИРОВКА**
```python
"""
📚 ТЕОРЕТИЧЕСКАЯ ОСНОВА: Группировка по возрасту, полу и категориям

ПРОБЛЕМА: Простые группировки не показывают пересечения

РЕШЕНИЕ: Многомерная группировка:
"GENDER_CATEGORY_Male_Electronics" → $45,000
"AGE_CATEGORY_25-34_Books" → $15,000  
"GENDER_AGE_CATEGORY_Female_35-44_Clothing" → $28,000

🔧 ТЕХНИКА:
- Двойные и тройные группировки
- Иерархические ключи
- Анализ пересечений демографии и продуктов
"""
```

### **6. `time_pattern_analysis.py` - ВРЕМЕННЫЕ ПАТТЕРНЫ**
```python
"""
📚 ТЕОРЕТИЧЕСКАЯ ОСНОВА: Продажи по дням/месяцам + временные паттерны

ПРОБЛЕМА: Временные данные без анализа паттернов

РЕШЕНИЕ: Многоуровневый временной анализ:
"WEEKDAY_Monday" → $45,200
"WEEKEND_1" → $120,500 (выходные)
"CATEGORY_SEASON_Electronics_SUMMER" → $89,000

🔧 ТЕХНИКА:
- Различные временные срезы: дни, недели, месяцы, сезоны
- Анализ будни/выходные
- Временные паттерны по категориям
"""
```

### **7. `revenue_dynamics.py` - ДИНАМИКА МЕТРИК**
```python
"""
📚 ТЕОРЕТИЧЕСКАЯ ОСНОВА: Динамика среднего чека и выручки

ПРОБЛЕМА: Статические метрики без трендов

РЕШЕНИЕ: Динамический анализ:
"СРЕДНИЙ_ЧЕК_2023-01" → $85.20
"ВЫРУЧКА_GENDER_CATEGORY_Male_Electronics" → $45,000
"GROWTH_BASE_2023-02" → +15%

🔧 ТЕХНИКА:
- Вычисление среднего чека
- Динамика по месяцам
- Кросс-анализ демографии и категорий
- Подготовка данных для анализа роста
"""
```

### **8. `comprehensive_time_analysis.py` - КОМПЛЕКСНЫЙ АНАЛИЗ**
```python
"""
📚 ТЕОРЕТИЧЕСКАЯ ОСНОВА: Полные пересечения измерений

ПРОБЛЕМА: Простые анализа не показывают полную картину

РЕШЕНИЕ: Комплексные паттерны:
"FULL_PATTERN_Male_25-34_Electronics_MORNING" → $12,500
"FULL_PATTERN_Female_35-44_Clothing_Saturday" → $8,200

🔧 ТЕХНИКА:
- Полные пересечения: Демография + Категория + Время
- Анализ времени суток: MORNING/AFTERNOON/EVENING/NIGHT
- Дни недели + время суток
- Максимальная детализация паттернов покупок
"""
```

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



### **2. СОСТАВНЫЕ КЛЮЧИ ДЛЯ МНОГОМЕРНОГО АНАЛИЗА**

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
                
                # 🔥 СОСТАВНЫЕ КЛЮЧИ - многомерный анализ в одном проходе
                
                # Временные срезы
                yield f"TIME_{year_month}", total_amount
                yield f"TIME_SEASON_{season}", total_amount
                
                # Демографические срезы  
                yield f"DEMO_GENDER_{gender}", total_amount
                yield f"DEMO_AGE_{age_group}", total_amount
                
                # Продуктовые срезы
                yield f"PRODUCT_{category}", total_amount
                
                # 🔥 КРОСС-СЕКЦИОННЫЕ АНАЛИЗЫ (составные ключи)
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

### **3. MULTIPLE OUTPUTS - КОМПЛЕКСНАЯ АНАЛИТИКА**

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
                
                # 🔥 MULTIPLE OUTPUTS В ОДНОМ MAPPER
                
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

### **4. ЦЕНОВАЯ ЭЛАСТИЧНОСТЬ (исправленная)**

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

---

## **🚀 ЗАПУСК ИСПРАВЛЕННЫХ СКРИПТОВ**

```bash
# Копируем исправленные скрипты в контейнер
docker cp secondary_sort.py namenode:/scripts/
docker cp composite_keys.py namenode:/scripts/ 
docker cp multiple_outputs.py namenode:/scripts/
docker cp real_price_elasticity.py namenode:/scripts/

# Запускаем ВНУТРИ контейнера
docker-compose exec namenode bash
export PATH="/tmp/python/bin:$PATH"
cd /scripts

# 1. Запускаем ВТОРИЧНУЮ СОРТИРОВКУ
python3 secondary_sort.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/secondary_sort

# 2. Запускаем СОСТАВНЫЕ КЛЮЧИ
python3 composite_keys.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/composite_keys

# 3. Запускаем MULTIPLE OUTPUTS
python3 multiple_outputs.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/multiple_outputs

# 4. Запускаем ЦЕНОВУЮ ЭЛАСТИЧНОСТЬ
python3 real_price_elasticity.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/price_elasticity
```

---




