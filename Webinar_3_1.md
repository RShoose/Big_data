
# Администрирование HDFS: теория и практика

##  План вебинара <a id="план-вебинара"></a>
### **1. [Архитектура и базовый мониторинг HDFS](#1)**
- Архитектура HDFS в схемах
- Ключевые концепции и инструменты мониторинга
- Практика: проверка состояния и топологии кластера

### **2. [Конфигурационные файлы HDFS](#2)**
- Архитектура конфигурации HDFS
- Детальные параметры конфигурации
- Практика: анализ и управление конфигурацией

### **3. [Диагностика и мониторинг HDFS](#3)**
- Инструменты диагностики HDFS
- Детальная работа с hdfs fsck
- Практика: комплексная диагностика и поиск проблем

### **4. [Управление ресурсами: квоты HDFS](#4)**
- Система квот HDFS
- Принципы работы и стратегии применения
- Практика: работа с space и namespace квотами

### **5. [Балансировка кластера HDFS](#5)**
- Принципы балансировки HDFS
- Механизм работы балансировщика
- Практика: создание дисбаланса и балансировка

### **6. [Q&A и резюме вебинара](#6)**
- Ключевые темы вебинара
- Типичные проблемы и решения
- Практика: финальная проверка и чеклист

---

<a name="1"></a>
## 1. Архитектура и базовый мониторинг HDFS

### **Теоретическая часть**

### **1.1. Архитектура HDFS в схемах**

#### **Компоненты кластера HDFS**
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   NameNode      │    │   DataNode      │    │   DataNode      │
│   (Master)      │    │   (Slave)       │    │   (Slave)       │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Метаданные    │◄---│ • Блоки данных  │    │ • Блоки данных  │
│ • FsImage       │    │ • Block reports │    │ • Block reports │
│ • EditLog       │    │ • Heartbeats    │    │ • Heartbeats    │
│ • BlockMap      │---►│ • Чтение/запись │    │ • Чтение/запись │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  ▼
                         ┌─────────────────┐
                         │    Клиенты      │
                         │  (Чтение/запись)│
                         └─────────────────┘
```

#### **Таблица компонентов HDFS**
| **Компонент** | **Роль** | **Хранит** | **Процессы** | **Порт** |
|---------------|----------|------------|--------------|----------|
| **NameNode** | Мастер-узел | Метаданные, FsImage, EditLog | Управление namespace, репликация | 9870 (Web) |
| **DataNode** | Slave-узел | Блоки данных, checksums | Чтение/запись, heartbeat | 9866 (Data) |
| **Secondary NN** | Помощник | Checkpoint'ы | Слияние FsImage+EditLog | 9868 |
| **JournalNode** | HA-поддержка | Общий EditLog | Синхронизация | 8485 |

### **1.2. Ключевые концепции**

#### **Схема репликации с Rack Awareness**
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Rack 1    │    │   Rack 2    │    │   Rack 3    │
├─────────────┤    ├─────────────┤    ├─────────────┤
│ • DataNode1 │    │ • DataNode3 │    │ • DataNode5 │
│   REPLICA 1 │    │   REPLICA 3 │    │   REPLICA 2 │
│ • DataNode2 │    │ • DataNode4 │    │ • DataNode6 │
└─────────────┘    └─────────────┘    └─────────────┘
```

#### **Таблица стратегии репликации**
| **Реплика** | **Размещение** | **Цель** | **Условия** |
|-------------|----------------|----------|-------------|
| **1-я** | Локальный узел/стойка | Минимизация сетевого трафика | Если клиент в кластере |
| **2-я** | Другая стойка | Отказоустойчивость | Разные стойки для баланса |
| **3-я** | Третья стойка | Высокая доступность | Географическое распределение |

### **1.3. Инструменты мониторинга**

#### **Таблица команд мониторинга**
| **Команда** | **Назначение** | **Ключевые метрики** | **Частота использования** |
|-------------|----------------|---------------------|--------------------------|
| **`hdfs dfsadmin -report`** | Общий отчет кластера | Live/Dead nodes, Capacity, Used% | Ежедневно |
| **`hdfs dfsadmin -printTopology`** | Топология сети | Rack layout, Node distribution | При изменении топологии |
| **`hdfs fsck /`** | Проверка целостности | Corrupt blocks, Under-replicated | Еженедельно/при проблемах |
| **`hdfs dfs -df -h`** | Использование пространства | Used/Remaining space | Постоянно |
| **`jps`** | Проверка процессов | Running Java processes | При перезапуске сервисов |

#### **Схема Web UI NameNode (:9870)**
```
┌─────────────────────────────────────────────────┐
│                NameNode Web UI                  │
├─────────────────┬───────────────────────────────┤
│   Cluster       │  • Configured Capacity: 500GB │
│   Overview      │  • DFS Used: 150GB (30%)      │
│                 │  • Live Nodes: 3/3            │
├─────────────────┼───────────────────────────────┤
│   DataNodes     │ ┌─────┬────────┬────────────┐ │
│   Information   │ │ Node│ Used%  │Last Contact│ │
│                 │ ├─────┼────────┼────────────┤ │
│                 │ │ DN1 │ 28%    │ 10s ago    │ │
├─────────────────┼───────────────────────────────┤
│   Snapshots     │  • Last fsck: 2024-01-15      │
│   & Utilities   │  • Safe Mode: OFF             │
└─────────────────┴───────────────────────────────┘
```

### **1.4. Критические метрики**

#### **Таблица ключевых метрик**
| **Категория** | **Метрика** | **Нормальное значение** | **Тревожное значение** |
|---------------|-------------|------------------------|------------------------|
| **Доступность** | Live Nodes | = общему количеству DN | < общего количества DN |
| **Емкость** | DFS Used% | < 80% | > 85% |
| **Целостность** | Under Replicated Blocks | 0 | > 0 |
| **Целостность** | Corrupt Blocks | 0 | > 0 |
| **Производительность** | Pending Replication Blocks | < 100 | > 1000 |

### **1.5. Процессы работы HDFS**

#### **Схема чтения данных**
```
Клиент → NameNode (get block locations) → DataNodes (read data directly)
    1. Запрос метаданных
    2. Получение списка DataNodes
    3. Прямое чтение с DataNodes
    4. Failover при ошибках
```

#### **Схема записи данных**
```
Клиент → NameNode (create file) → DataNodes (pipeline write) → Подтверждение
    1. Создание файла в метаданных
    2. Получение конвейера DataNodes
    3. Поточная запись с репликацией
    4. Подтверждение операции
```

### **Практическая часть**

```bash
# Проверка состояния процессов
echo "=== ПРОВЕРКА КЛАСТЕРА ==="
docker exec namenode jps
docker exec datanode1 jps
docker exec -it namenode bash

# Базовый мониторинг кластера
echo "=== Общий отчет кластера ==="
hdfs dfsadmin -report

# Анализ топологии
echo "=== Топология кластера ==="
hdfs dfsadmin -printTopology

# Работа с Web UI
echo "Web UI доступен по: http://$(hostname -I | awk '{print $1}'):9870"
```

[🔼 Наверх](#план-вебинара)

---

<a name="2"></a>
## 2. Конфигурационные файлы HDFS

### **Теоретическая часть**

### **2.1. Архитектура конфигурации HDFS**

#### **Схема иерархии конфигурационных файлов**
```
┌─────────────────────────────────────────────────┐
│            Hadoop Configuration                 │
├─────────────────┬───────────────────────────────┤
│   core-site.xml │   hdfs-site.xml               │
│   • Общие       │   • Специфичные HDFS          │
│   настройки     │   настройки                   │
├─────────────────┼───────────────────────────────┤
│   workers       │   hadoop-env.sh               │
│   • Список      │   • Переменные окружения      │
│   DataNodes     │   • Параметры JVM             │
└─────────────────┴───────────────────────────────┘
```

#### **Таблица конфигурационных файлов**
| **Файл** | **Назначение** | **Ключевые параметры** | **Расположение** |
|----------|----------------|------------------------|------------------|
| **core-site.xml** | Общие настройки кластера | `fs.defaultFS`, `hadoop.tmp.dir` | `/etc/hadoop/conf/` |
| **hdfs-site.xml** | Специфичные настройки HDFS | `dfs.replication`, `dfs.blocksize` | `/etc/hadoop/conf/` |
| **workers** | Список DataNodes | hostnames DataNodes | `/etc/hadoop/conf/` |
| **hadoop-env.sh** | Переменные окружения | `JAVA_HOME`, `HADOOP_HEAPSIZE` | `/etc/hadoop/conf/` |

### **2.2. Детальные параметры конфигурации**

#### **core-site.xml - Критические параметры**
```xml
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://namenode:9820</value>
</property>
<property>
  <name>hadoop.tmp.dir</name>
  <value>/opt/hadoop/tmp</value>
</property>
```

#### **hdfs-site.xml - Параметры NameNode**
```xml
<property>
  <name>dfs.namenode.name.dir</name>
  <value>file:///opt/hadoop/dfs/name</value>
</property>
<property>
  <name>dfs.permissions.enabled</name>
  <value>true</value>
</property>
```

### **Практическая часть**

**Практическая часть: Добавление конфигурационных файлов в Hadoop-кластер**

## Подготовка
```bash
cd ~/hadoop-clusters
mkdir -p custom-configs
cd ~/hadoop-clusters/custom-configs
```

## 1. Создаем конфигурационные файлы

### core-site.xml
```bash
cat > core-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/tmp/hadoop-custom</value>
  </property>
  <property>
    <name>custom.setting</name>
    <value>webinar_demo</value>
  </property>
</configuration>
EOF
```

### hdfs-site.xml
```bash
cat > hdfs-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.blocksize</name>
    <value>67108864</value>
  </property>
</configuration>
EOF
```

## 2. Копируем конфиги в основные узлы

```bash
# Копируем в NameNode и DataNodes
docker cp core-site.xml namenode:/opt/hadoop-3.2.1/etc/hadoop/core-site.xml
docker cp hdfs-site.xml namenode:/opt/hadoop-3.2.1/etc/hadoop/hdfs-site.xml

docker cp core-site.xml datanode1:/opt/hadoop-3.2.1/etc/hadoop/core-site.xml
docker cp hdfs-site.xml datanode1:/opt/hadoop-3.2.1/etc/hadoop/hdfs-site.xml

docker cp core-site.xml datanode2:/opt/hadoop-3.2.1/etc/hadoop/core-site.xml
docker cp hdfs-site.xml datanode2:/opt/hadoop-3.2.1/etc/hadoop/hdfs-site.xml

docker cp core-site.xml datanode3:/opt/hadoop-3.2.1/etc/hadoop/core-site.xml
docker cp hdfs-site.xml datanode3:/opt/hadoop-3.2.1/etc/hadoop/hdfs-site.xml

docker cp core-site.xml resourcemanager:/opt/hadoop-3.2.1/etc/hadoop/core-site.xml
docker cp hdfs-site.xml resourcemanager:/opt/hadoop-3.2.1/etc/hadoop/hdfs-site.xml

echo "Конфиги скопированы в основные узлы"
```

## 3. Тестируем применение конфигов

### Проверка настроек
```bash
echo "=== Проверка настроек ==="
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs getconf -confKey hadoop.tmp.dir
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs getconf -confKey custom.setting
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs getconf -confKey dfs.replication
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs getconf -confKey dfs.blocksize
```

### Проверка работы кластера
```bash
echo "=== Проверка работы кластера ==="

# Процессы
docker exec namenode jps
docker exec datanode1 jps

# Статус HDFS
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfsadmin -report | grep "Live datanodes"

# Тестовая операция
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /custom-test
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -put /opt/hadoop-3.2.1/README.txt /custom-test/
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -ls /custom-test/

# Проверка информации о файле
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs fsck /custom-test/README.txt -blocks -locations
```

## 4. Итоговая проверка
```bash
echo "=== Итоговая проверка ==="
echo "Настройки применены:"
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs getconf -confKey custom.setting
echo "Кластер работает:"
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -test -e /custom-test/README.txt && echo "Файл в HDFS: OK" || echo "Ошибка"
echo "Процессы:"
docker exec namenode jps | grep -E "(NameNode|DataNode)"
```

**Готово! Конфигурационные файлы добавлены из папки custom-configs и проверены.**

[🔼 Наверх](#план-вебинара)

---

<a name="3"></a>
## 3. Диагностика и мониторинг HDFS

### **Теоретическая часть**

### **3.1. Инструменты диагностики HDFS**

#### **Схема диагностического workflow**
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Быстрая       │    │   Детальная     │    │   Визуальный    │
│   проверка      │    │   диагностика   │    │   мониторинг    │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • dfsadmin      │ →  │ • fsck          │ →  │ • Web UI        │
│   -report       │    │   -files        │    │   :9870         │
│ • dfs -df -h    │    │   -blocks       │    │ • JMX метрики   │
└─────────────────┘    │   -locations    │    └─────────────────┘
                       └─────────────────┘
```

#### **Таблица инструментов диагностики**
| **Инструмент** | **Назначение** | **Ключевые метрики** | **Когда использовать** |
|----------------|----------------|---------------------|------------------------|
| **`hdfs fsck`** | Проверка целостности данных | Corrupt blocks, Under-replicated | При проблемах с данными |
| **`hdfs dfsadmin`** | Мониторинг состояния | Live nodes, Capacity | Ежедневный мониторинг |
| **Web UI** | Визуальный анализ | Graphs, Node details | Постоянный мониторинг |
| **JMX интерфейс** | Метрики производительности | RPC stats, Memory | Глубокий анализ |

### **Практическая часть**

```bash
# Базовая проверка здоровья файловой системы
hdfs fsck /

# Детальная проверка с информацией о файлах и блоках
hdfs fsck / -files -blocks -locations

# Поиск поврежденных блоков
hdfs fsck / -list-corruptfileblocks

# Проверка конкретной директории
hdfs fsck /user/important-data -files -blocks

# Только статистика без деталей
hdfs fsck / -files -blocks 2>/dev/null | grep -E "files|blocks|Healthy|Missing|Corrupt"

# Полный отчет о состоянии кластера
hdfs dfsadmin -report

# Краткая сводка ключевых метрик
hdfs dfsadmin -report | grep -E "Live|Dead|Configured Capacity|DFS Used"

# Проверка использования пространства
hdfs dfs -df -h

# Статус safe mode
hdfs dfsadmin -safemode get

# Топология кластера
hdfs dfsadmin -printTopology

# Мониторинг в реальном времени (обновление каждые 5 сек)
watch -n 5 "hdfs dfsadmin -report | head -20"
```

[🔼 Наверх](#план-вебинара)

---

<a name="4"></a>
## 4. Управление ресурсами: квоты HDFS

### **Теоретическая часть**

### **4.1. Система квот HDFS**

#### **Схема работы квот**
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Space Quota   │    │  Namespace      │    │   Storage       │
│   (Байты)       │    │  Quota          │    │   Type Quota    │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Ограничение   │    │ • Ограничение   │    │ • Ограничение   │
│   дискового     │    │   количества    │    │   по типам      │
│   пространства  │    │   файлов и      │    │   хранения      │
│ • Жесткое       │    │   директорий    │    │ • SSD, DISK,    │
│   ограничение   │    │ • Жесткое       │    │   ARCHIVE       │
└─────────────────┘    │   ограничение   │    └─────────────────┘
                       └─────────────────┘
```

#### **Таблица типов квот**
| **Тип квоты** | **Единица измерения** | **Что ограничивает** | **Команда установки** |
|---------------|----------------------|---------------------|----------------------|
| **Space Quota** | Байты | Дисковое пространство | `hdfs dfsadmin -setSpaceQuota` |
| **Namespace Quota** | Количество объектов | Файлы и директории | `hdfs dfsadmin -setQuota` |
| **Storage Type Quota** | Байты по типам | Пространство по типам хранения | `hdfs dfs -setSpaceQuota` |

**Практическое задание: Квоты в HDFS**

## Цель: Освоить установку и управление квотами дискового пространства

### 1. Подготовка
```bash
docker exec -it namenode bash
hdfs dfs -mkdir /quota_demo
hdfs dfs -mkdir /quota_demo/small
hdfs dfs -mkdir /quota_demo/large
```

### 2. Установка квот
```bash
# Маленькая квота (20MB)
hdfs dfsadmin -setSpaceQuota 20M /quota_demo/small

# Большая квота (200MB)  
hdfs dfsadmin -setSpaceQuota 200M /quota_demo/large

# Проверка
hdfs dfs -count -q /quota_demo/small
hdfs dfs -count -q /quota_demo/large
```

### 3. Тестирование ограничений
```bash
# Создаем тестовые файлы
dd if=/dev/zero of=/tmp/test1.bin bs=1M count=5
dd if=/dev/zero of=/tmp/test2.bin bs=1M count=15

# Тест small квоты (должна быть ошибка)
hdfs dfs -put /tmp/test1.bin /quota_demo/small/

# Тест large квоты (должен работать)
hdfs dfs -put /tmp/test1.bin /quota_demo/large/
hdfs dfs -put /tmp/test2.bin /quota_demo/large/
```

### 4. Решение проблемы блоков
```bash
# Увеличиваем квоту
hdfs dfsadmin -setSpaceQuota 150M /quota_demo/small

# Теперь файл должен поместиться
hdfs dfs -put /tmp/test1.bin /quota_demo/small/

# Проверяем результат
hdfs dfs -count -q /quota_demo/small
hdfs dfs -du -h /quota_demo
```

### 5. Управление квотами
```bash
# Снимаем квоту
hdfs dfsadmin -clrSpaceQuota /quota_demo/small

# Проверяем
hdfs dfs -count -q /quota_demo/small
```


[🔼 Наверх](#план-вебинара)

---

<a name="5"></a>
## 5. Балансировка кластера HDFS

### **Теоретическая часть**

### **5.1. Принципы балансировки HDFS**

#### **Схема дисбаланса и балансировки**
```
ДО балансировки:
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  DataNode1  │  │  DataNode2  │  │  DataNode3  │
│   75% used  │  │   45% used  │  │   30% used  │
│   ███████   │  │   ████      │  │   ███       │
└─────────────┘  └─────────────┘  └─────────────┘

ПОСЛЕ балансировки:
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  DataNode1  │  │  DataNode2  │  │  DataNode3  │
│   50% used  │  │   50% used  │  │   50% used  │
│   █████     │  │   █████     │  │   █████     │
└─────────────┘  └─────────────┘  └─────────────┘
```

#### **Таблица параметров балансировки**
| **Параметр** | **Значение по умолчанию** | **Описание** | **Рекомендации** |
|--------------|--------------------------|--------------|------------------|
| **threshold** | 10% | Максимальное отклонение от среднего | 5-15% в зависимости от нагрузки |
| **bandwidthPerSec** | 10 МБ/с | Скорость передачи | 50-100 МБ/с для быстрой балансировки |
| **exclude** | - | Исключаемые узлы | Узлы в maintenance |
| **include** | - | Включаемые узлы | Только определенные узлы |

**Практическое задание: Балансировка и мониторинг HDFS**

## Цель: Освоить балансировку кластера и мониторинг производительности

### 1. Подготовка тестовых данных
```bash
# Создаем тестовые данные разного размера
dd if=/dev/urandom of=/tmp/data_100mb.bin bs=1M count=100
dd if=/dev/urandom of=/tmp/data_50mb.bin bs=1M count=50
dd if=/dev/urandom of=/tmp/data_200mb.bin bs=1M count=200

# Создаем директории для теста
hdfs dfs -mkdir /balance_test
hdfs dfs -mkdir /balance_test/node1
hdfs dfs -mkdir /balance_test/node2
```

### 2. Создаем искусственный дисбаланс
```bash
# Загружаем данные в разные директории (имитация неравномерной загрузки)
hdfs dfs -put /tmp/data_100mb.bin /balance_test/node1/
hdfs dfs -put /tmp/data_200mb.bin /balance_test/node1/
hdfs dfs -put /tmp/data_50mb.bin /balance_test/node2/

# Проверяем распределение
hdfs dfs -du -h /balance_test
```

### 3. Мониторинг перед балансировкой
```bash
# Проверяем текущее распределение данных
echo "=== Распределение по узлам ==="
hdfs dfsadmin -report | grep -A 5 "Datanode" | grep -E "Name|HostName|DFS Used"

# Детальная статистика
hdfs fsck / -blocks | grep "Total blocks"
```

### 4. Запуск балансировки
```bash
# Балансировка с ограничением пропускной способности
hdfs balancer -D dfs.datanode.balance.bandwidthPerSec=10485760 -threshold 5

# Мониторинг процесса в реальном времени (в другом терминале)
# hdfs dfsadmin -report | grep "DFS Used%"
```

### 5. Проверка результатов
```bash
# Сравниваем распределение до и после
echo "=== После балансировки ==="
hdfs dfsadmin -report | grep -A 5 "Datanode" | grep -E "Name|DFS Used"

# Проверяем целостность данных
hdfs fsck /balance_test -blocks -locations

# Общая статистика
hdfs dfsadmin -report | grep -E "Configured Capacity|Present Capacity|DFS Used%"
```

### 6. Тест под нагрузкой
```bash
# Создаем дополнительную нагрузку
for i in {1..5}; do
    hdfs dfs -put /tmp/data_50mb.bin /balance_test/stress_$i.bin &
done
wait

# Проверяем распределение под нагрузкой
hdfs dfsadmin -report | grep "DFS Used%"
```

### 7. Очистка тестовых данных
```bash
# Удаляем тестовые данные
hdfs dfs -rm -r /balance_test
rm -f /tmp/data_*.bin

# Финальная проверка баланса
hdfs balancer -threshold 5
```

[🔼 Наверх](#план-вебинара)

---

<a name="6"></a>
## 6. Q&A и резюме вебинара

### **Теоретическая часть**

### **6.1. Ключевые темы вебинара**

#### **Схема администрирования HDFS**
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Мониторинг    │    │  Конфигурация   │    │  Управление     │
│   и диагностика │    │   и настройки   │    │   ресурсами     │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • dfsadmin      │    │ • core-site.xml │    │ • Space Quotas  │
│ • fsck          │    │ • hdfs-site.xml │    │ • Namespace     │
│ • Web UI        │    │ • workers       │    │   Quotas        │
│ • jps           │    │ • getconf       │    │ • Балансировка  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

#### **Таблица освоенных навыков**
| **Навык** | **Команды** | **Назначение** | **Частота использования** |
|-----------|-------------|----------------|--------------------------|
| **Мониторинг** | `dfsadmin -report`, `fsck` | Контроль состояния кластера | Ежедневно |
| **Конфигурация** | `getconf`, редактирование XML | Настройка параметров | При изменениях |
| **Управление квотами** | `setSpaceQuota`, `setQuota` | Контроль ресурсов | По необходимости |
| **Балансировка** | `balancer` | Выравнивание нагрузки | После изменений кластера |

### **6.2. Типичные проблемы и решения**

#### **Таблица распространенных проблем**
| **Проблема** | **Симптомы** | **Диагностика** | **Решение** |
|-------------|--------------|-----------------|-------------|
| **Недостаток места** | Ошибки записи | `dfsadmin -report` | Очистка данных, квоты |
| **Потеря узлов** | Dead Nodes | `dfsadmin -report` | Перезапуск сервисов |
| **Поврежденные блоки** | Corrupt blocks | `fsck -list-corruptfileblocks` | Восстановление из реплик |
| **Дисбаланс** | Разное использование узлов | `dfsadmin -report` | Запуск балансировщика |



[🔼 Наверх](#план-вебинара)

---

## Итоги вебинара

### **Освоенные навыки:**
- Мониторинг состояния кластера HDFS
- Работа с конфигурационными файлами
- Диагностика и устранение проблем
- Управление квотами и ресурсами
- Балансировка кластера

### **Ключевые команды:**
```bash
hdfs dfsadmin -report          # Общий мониторинг
hdfs fsck / -files -blocks     # Диагностика целостности
hdfs dfsadmin -setSpaceQuota   # Установка квот
hdfs balancer -threshold 10    # Балансировка кластера
```





