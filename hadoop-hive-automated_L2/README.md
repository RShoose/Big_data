## Инструкция установки Hadoop + Hive кластера

### Требуемая структура файлов:

```
hadoop-hive-cluster/
├── apache-hive-2.3.9-bin.tar.gz    # Скачать командой ниже
├── guava-27.0-jre.jar              # Скачать командой ниже
├── Dockerfile.hive                  # Создать с содержимым ниже
├── docker-compose.yml               # Создать с содержимым ниже
└── entrypoint.sh                    # Создать с содержимым ниже
```

### 1. Подготовка директории и скачивание файлов

```bash
mkdir hadoop-hive-cluster
cd hadoop-hive-cluster

# Скачать Hive 2.3.9
wget https://archive.apache.org/dist/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz

# Скачать Guava
wget -O guava-27.0-jre.jar \
  https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar
```

### 2. Создать `Dockerfile.hive`:

```dockerfile
FROM bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8

COPY apache-hive-2.3.9-bin.tar.gz /tmp/
COPY guava-27.0-jre.jar /tmp/

RUN tar -xzf /tmp/apache-hive-2.3.9-bin.tar.gz -C /opt && \
    mv /opt/apache-hive-2.3.9-bin /opt/hive && \
    rm /tmp/apache-hive-2.3.9-bin.tar.gz

RUN rm -f /opt/hive/lib/guava-*.jar && \
    rm -f /opt/hadoop-3.2.1/share/hadoop/*/lib/guava-*.jar 2>/dev/null || true && \
    cp /tmp/guava-27.0-jre.jar /opt/hive/lib/ && \
    cp /tmp/guava-27.0-jre.jar /opt/hadoop-3.2.1/share/hadoop/common/lib/

ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV HIVE_HOME=/opt/hive
ENV PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH

RUN mkdir -p /opt/hive/conf

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
```

### 3. Создать `docker-compose.yml`:

```yaml
version: '3.7'

services:
  namenode-lecture:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode-lecture
    ports:
      - "9871:9870"
    environment:
      - CLUSTER_NAME=test

  datanode-lecture:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode-lecture
    depends_on:
      - namenode-lecture
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://namenode-lecture:8020

  hive-lecture:
    image: hive-derby:latest
    container_name: hive-lecture
    depends_on:
      - namenode-lecture
      - datanode-lecture
    volumes:
      - ./guava-27.0-jre.jar:/host/guava.jar:ro
      - ./data:/data:ro
    ports:
      - "10001:10000"
```

### 4. Создать `entrypoint.sh`:

```bash
#!/bin/bash
set -e

echo "=== Настройка Hive для лекции ==="

echo "Копирование guava..."
cp /host/guava.jar /opt/hive/lib/guava-27.0-jre.jar
cp /host/guava.jar /opt/hadoop-3.2.1/share/hadoop/common/lib/

rm -f /opt/hadoop-3.2.1/share/hadoop/hdfs/lib/guava-*.jar 2>/dev/null || true

mkdir -p /opt/hive/conf
cat > /opt/hive/conf/hive-site.xml << 'XML'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=/tmp/lecture_metastore;create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>hdfs://namenode-lecture:8020/user/hive/warehouse</value>
  </property>
</configuration>
XML

mkdir -p /opt/hadoop/etc/hadoop
cat > /opt/hadoop/etc/hadoop/core-site.xml << 'XML'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode-lecture:8020</value>
  </property>
</configuration>
XML

echo "Ожидание HDFS..."
sleep 45

echo "Инициализация метастора Hive..."
cd /opt/hive/bin
./schematool -initSchema -dbType derby

echo ""
echo "=== Hive успешно настроен ==="
echo "Для использования выполните:"
echo "  docker exec -it hive-lecture /opt/hive/bin/hive"
echo ""
echo "Пример команд:"
echo "  SHOW DATABASES;"
echo "  CREATE DATABASE demo;"
echo "  USE demo;"
echo "  CREATE TABLE users (id INT, name STRING);"
echo ""

tail -f /dev/null
```

### 5. Установка и запуск:

```bash
# 1. Собрать образ Hive
docker build -f Dockerfile.hive -t hive-derby:latest .

# 2. Запустить кластер
docker-compose up -d

# 3. Проверить статус
docker-compose ps

# Ожидаемый вывод:
# NAME                STATUS              PORTS
# namenode-lecture    Up X minutes        9871/tcp
# datanode-lecture    Up X minutes        
# hive-lecture        Up X minutes        10001/tcp
```

### 6. Использование Hive:

```bash
# Подключиться к Hive
docker exec -it hive-lecture /opt/hive/bin/hive

# Внутри Hive выполнить:
SHOW DATABASES;
CREATE DATABASE test;
USE test;
CREATE TABLE sample (id INT, name STRING);
INSERT INTO sample VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM sample;
```

### 7. Проверка HDFS:

```bash
# Проверить HDFS
docker exec namenode-lecture hdfs dfs -ls /

# Создать директорию в HDFS
docker exec namenode-lecture hdfs dfs -mkdir -p /user/hive/warehouse
```

### 8. Остановка кластера:

```bash
# Остановить
docker-compose stop

# Остановить и удалить
docker-compose down

# Полная очистка
docker-compose down -v
docker rmi hive-derby:latest
```

Если не инициализирован метастор

## **Очистка существующего метастора и повторная инициализация**

```bash
# Остановить все процессы Hive если они работают
pkill -f hive 2>/dev/null || true
pkill -f derby 2>/dev/null || true

# Удалить старый метастор
rm -rf /tmp/lecture_metastore
rm -rf /opt/hive/metastore_db
rm -f derby.log

# Перейти в директорию Hive
cd /opt/hive/bin

# Инициализировать метастор заново
./schematool -initSchema -dbType derby

# Если не работает, попробовать с verbose
./schematool -initSchema -dbType derby --verbose
```

## **Если метастор уже работает, просто запустить Hive**

```bash
# Проверить, может метастор уже работает
netstat -tlnp 2>/dev/null | grep 9083 || echo "Metastore не запущен"

# Если метастор не запущен, запустить его
cd /opt/hive/bin
./hive --service metastore > /tmp/metastore.log 2>&1 &

# Подождать 5 секунд
sleep 5

# Проверить логи
tail -20 /tmp/metastore.log

# Запустить Hive CLI
./hive
```

## **Использовать другой путь для метастора**

```bash
# Удалить старый
rm -rf /tmp/lecture_metastore

# Создать новый в другом месте
cd /opt/hive/bin
./schematool -initSchema -dbType derby -url "jdbc:derby:;databaseName=/tmp/new_metastore;create=true"

# Или указать в конфиге
cat > /opt/hive/conf/hive-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=/tmp/new_hive_metastore;create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>APP</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>mine</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>
</configuration>
EOF

# Затем инициализировать
./schematool -initSchema -dbType derby
```

## **Решение 4: Быстрый обходной путь - пропустить инициализацию**

```bash
# Просто попробовать запустить Hive без инициализации
cd /opt/hive/bin
./hive

# Если выдает ошибку, попробовать запустить в embedded режиме
./hive --hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=/tmp/embedded_metastore;create=true
```

## **Самый простой вариант**

```bash
# 1. Остановить всё
pkill -f hive 2>/dev/null || true
pkill -f derby 2>/dev/null || true

# 2. Очистить всё
rm -rf /tmp/*metastore*
rm -rf /opt/hive/metastore_db
rm -f derby.log

# 3. Запустить простой скрипт инициализации
cd /opt/hive/bin

# 4. Создать минимальную конфигурацию
cat > /tmp/hive_init.sh << 'EOF'
#!/bin/bash
echo "=== Очистка предыдущих данных ==="
rm -rf /tmp/metastore_db /tmp/lecture_metastore
sleep 2

echo "=== Инициализация Hive Metastore ==="
cd /opt/hive/bin
./schematool -initSchema -dbType derby 2>&1 | grep -i -A3 -B3 "success\|fail\|error" || true

echo "=== Запуск Hive ==="
./hive
EOF

chmod +x /tmp/hive_init.sh
/tmp/hive_init.sh
```

## **Если ничего не помогает - запустить с skip**

```bash
# Проверить, может таблицы уже созданы
cd /opt/hive/bin
./schematool -info -dbType derby

# Если показывает версию схемы, значит всё уже инициализировано
# Тогда просто запустить Hive
./hive
```

## **Еще один способ:**

```bash
# Просто выполните эти команды по порядку:

# 1. Очистить
rm -rf /tmp/lecture_metastore

# 2. Перейти в директорию Hive
cd /opt/hive/bin

# 3. Попробовать запустить Hive без инициализации
./hive

# Если не работает, тогда:

# 4. Принудительно инициализировать
./schematool -initSchema -dbType derby --verbose 2>&1 | tail -30

# 5. Или использовать force
./schematool -dbType derby -initSchemaTo 2.3.0
```

**Самый вероятный сценарий**: Mетастор уже был инициализирован в предыдущих запусках. Просто запустите Hive:

```bash
hive
```

