```markdown
# Подготовка данных для Hadoop-Hive кластера

[Источник](https://www.kaggle.com/datasets/mikhailklemin/kinopoisks-movies-reviews)
## Обзор
Инструкция по подготовке текстовых данных (TXT файлов) из вашей Windows папки для использования в Hadoop-Hive кластере.

## Быстрая подготовка данных

### Шаг 1: Установите путь к вашим данным

Откройте терминал и задайте переменную с путем к вашей Windows папке:

```bash
MY_DATA_PATH="/mnt/d/ваша_папка/с_данными/dataset"
```

Например:
```bash
MY_DATA_PATH="/mnt/c/Users/ваше_имя/Documents/dataset"
```

### Шаг 2: Проверьте путь

```bash
# Проверьте, существует ли путь
if [ -d "$MY_DATA_PATH" ]; then
    echo "Путь найден: $MY_DATA_PATH"
    ls "$MY_DATA_PATH"
else
    echo "Путь не найден: $MY_DATA_PATH"
    echo "Проверьте правильность пути и перезапустите скрипт"
    exit 1
fi
```

### Шаг 3: Создайте директории для данных

```bash
# Создаем структуру в проекте
LECTURE_DATA="data/lecture_reviews"
mkdir -p "$LECTURE_DATA/pos"
mkdir -p "$LECTURE_DATA/neg"
mkdir -p "$LECTURE_DATA/neu"
```

### Шаг 4: Копируйте TXT файлы

```bash
echo "Копирование TXT файлов из $MY_DATA_PATH..."

for sentiment in pos neg neu; do
    echo "  Категория: $sentiment"

    # Проверяем, существует ли исходная папка
    if [ ! -d "$MY_DATA_PATH/$sentiment" ]; then
        echo "    Папка не найдена: $MY_DATA_PATH/$sentiment"
        continue
    fi

    # Создаем целевую папку если не существует
    mkdir -p "$LECTURE_DATA/$sentiment/"

    # Копируем первые 10 файлов
    count=0
    for file in "$MY_DATA_PATH/$sentiment"/*.txt; do
        # Проверка что file действительно файл (не шаблон)
        if [ "$file" = "$MY_DATA_PATH/$sentiment/*.txt" ]; then
            break  # Нет файлов по шаблону
        fi
        
        if [ -f "$file" ] && [ $count -lt 20 ]; then
            cp "$file" "$LECTURE_DATA/$sentiment/"
            echo "    Скопирован: $(basename "$file")"
            ((count++))
        elif [ $count -ge 20 ]; then
            break  # Прерываем когда скопировали 10 файлов
        fi
    done

    if [ $count -eq 0 ]; then
        echo "    Нет TXT файлов"
    else
        echo "    Итого скопировано: $count файлов"
    fi
done

echo "Копирование завершено!"
```

### Шаг 5: Проверьте результат

```bash
echo ""
echo "=== РЕЗУЛЬТАТ ==="
echo "Структура данных:"
tree "$LECTURE_DATA" 2>/dev/null || ls -laR "$LECTURE_DATA"

echo ""
echo "Всего TXT файлов:"
find "$LECTURE_DATA" -name "*.txt" -type f | wc -l
```


## Проверка результата

После выполнения скрипта проверьте данные:
```bash
# Просмотр структуры
ls -la data/lecture_reviews/

# Проверка содержимого
head -3 data/lecture_reviews/pos/*.txt

# Подсчет файлов
find data/lecture_reviews -name "*.txt" | wc -l
```

```

