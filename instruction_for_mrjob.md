## Полная инструкция от нуля до работающего Hadoop кластера

### 0. Предварительные требования

#### Проверка и установка Docker
```bash
# проверка Docker
docker --version
docker ps
```

### 1. Создание базовой структуры проекта

```bash
# создание основной директории 
mkdir -p ~/hadoop_retail_analysis
cd ~/hadoop_retail_analysis

# создание структуры каталогов
mkdir -p config data/input data/output scripts logs results

ls -la data/input/
# убедитесь, что retail_sales_dataset.csv существует в data/input/
```

### 2. Создание конфигурационных файлов

#### config/hadoop.env
```bash
cat > config/hadoop.env << 'EOF'
CORE_CONF_fs_defaultFS=hdfs://namenode:9000
CORE_CONF_hadoop_http_staticuser_user=root
CORE_CONF_hadoop_proxyuser_root_hosts=*
CORE_CONF_hadoop_proxyuser_root_groups=*

HDFS_CONF_dfs_webhdfs_enabled=true
HDFS_CONF_dfs_permissions_enabled=false
HDFS_CONF_dfs_namenode_datanode_registration_ip_hostname_check=false

YARN_CONF_yarn_log_aggregation_enable=true
YARN_CONF_yarn_resourcemanager_recovery_enabled=true
YARN_CONF_yarn_resourcemanager_store_class=org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore
YARN_CONF_yarn_resourcemanager_fs_stateStore_uri=/rmstate
YARN_CONF_yarn_nodemanager_remote_app_log_dir=/app-logs
YARN_CONF_yarn_nodemanager_aux-services=mapreduce_shuffle
YARN_CONF_yarn_nodemanager_aux-services_mapreduce_shuffle_class=org.apache.hadoop.mapred.ShuffleHandler
YARN_CONF_yarn_log_server_url=http://historyserver:8188/applicationhistory/logs/

MAPRED_CONF_mapreduce_framework_name=yarn
MAPRED_CONF_mapreduce_jobhistory_address=historyserver:10020
MAPRED_CONF_mapreduce_jobhistory_webapp_address=historyserver:19888
MAPRED_CONF_yarn_app_mapreduce_am_env=HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1
MAPRED_CONF_mapreduce_map_env=HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1
MAPRED_CONF_mapreduce_reduce_env=HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1
EOF
```

#### docker-compose.yml
```bash
cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    restart: always
    ports:
      - 9871:9870
      - 9001:9000
    volumes:
      - hadoop_namenode:/hadoop/dfs/name
      - ./data:/data
      - ./scripts:/scripts
    environment:
      - CLUSTER_NAME=retail_cluster
    env_file:
      - ./config/hadoop.env

  datanode1:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode1
    restart: always
    volumes:
      - hadoop_datanode1:/hadoop/dfs/data
      - ./data:/data
      - ./scripts:/scripts
    environment:
      - SERVICE_PRECONDITION="namenode:9000 namenode:9870"
    env_file:
      - ./config/hadoop.env
    depends_on:
      - namenode

  datanode2:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode2
    restart: always
    volumes:
      - hadoop_datanode2:/hadoop/dfs/data
      - ./data:/data
      - ./scripts:/scripts
    environment:
      - SERVICE_PRECONDITION="namenode:9000 namenode:9870"
    env_file:
      - ./config/hadoop.env
    depends_on:
      - namenode

  datanode3:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode3
    restart: always
    volumes:
      - hadoop_datanode3:/hadoop/dfs/data
      - ./data:/data
      - ./scripts:/scripts
    environment:
      - SERVICE_PRECONDITION="namenode:9000 namenode:9870"
    env_file:
      - ./config/hadoop.env
    depends_on:
      - namenode

  resourcemanager:
    image: bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8
    container_name: resourcemanager
    restart: always
    ports:
      - 8088:8088
      - 8032:8032
    volumes:
      - ./scripts:/scripts
    environment:
      - SERVICE_PRECONDITION="namenode:9000 namenode:9870 datanode1:9864 datanode2:9864 datanode3:9864"
    env_file:
      - ./config/hadoop.env
    depends_on:
      - namenode
      - datanode1
      - datanode2
      - datanode3

  nodemanager1:
    image: bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8
    container_name: nodemanager1
    restart: always
    volumes:
      - ./scripts:/scripts
    environment:
      - SERVICE_PRECONDITION="namenode:9000 namenode:9870 datanode1:9864 resourcemanager:8032"
    env_file:
      - ./config/hadoop.env
    depends_on:
      - resourcemanager

  nodemanager2:
    image: bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8
    container_name: nodemanager2
    restart: always
    volumes:
      - ./scripts:/scripts
    environment:
      - SERVICE_PRECONDITION="namenode:9000 namenode:9870 datanode2:9864 resourcemanager:8032"
    env_file:
      - ./config/hadoop.env
    depends_on:
      - resourcemanager

  nodemanager3:
    image: bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8
    container_name: nodemanager3
    restart: always
    volumes:
      - ./scripts:/scripts
    environment:
      - SERVICE_PRECONDITION="namenode:9000 namenode:9870 datanode3:9864 resourcemanager:8032"
    env_file:
      - ./config/hadoop.env
    depends_on:
      - resourcemanager

  historyserver:
    image: bde2020/hadoop-historyserver:2.0.0-hadoop3.2.1-java8
    container_name: historyserver
    restart: always
    ports:
      - 8188:8188
    volumes:
      - ./scripts:/scripts
    environment:
      - SERVICE_PRECONDITION="namenode:9000 namenode:9870 datanode1:9864 datanode2:9864 datanode3:9864 resourcemanager:8032"
    env_file:
      - ./config/hadoop.env
    depends_on:
      - resourcemanager

volumes:
  hadoop_namenode:
  hadoop_datanode1:
  hadoop_datanode2:
  hadoop_datanode3:
EOF
```

### 3. Запуск Hadoop кластера

```bash
# запуск всех сервисов
cd ~/hadoop_retail_analysis
docker-compose up -d
docker-compose ps

echo "Ждем запуска кластера..."
sleep 180

# проверяем логи namenode
docker-compose logs namenode | tail -20
```

### 4. Настройка HDFS

```bash
docker-compose exec namenode bash

# создание системных директорий в HDFS
hdfs dfs -mkdir -p /user/root/input
hdfs dfs -mkdir -p /user/root/output
hdfs dfs -mkdir -p /tmp
hdfs dfs -mkdir -p /tmp/mrjob

# копирование данных в контейнер
mkdir -p /data/input
exit

# из хостовой системы копируем файл в контейнер
docker cp ~/hadoop_retail_analysis/data/input/retail_sales_dataset.csv namenode:/data/input/

# снова заходим в namenode
docker-compose exec namenode bash

# загрузка данных в HDFS
hdfs dfs -put /data/input/retail_sales_dataset.csv /user/root/input/

# проверка загрузки
hdfs dfs -ls /user/root/input/
hdfs dfs -count /user/root/input/retail_sales_dataset.csv

# проверка статуса HDFS
hdfs dfsadmin -report
```

### 5. Установка Python и MRJob на все узлы

#### Установка на namenode
```bash
# в контейнере namenode
cd /tmp

# скачивание и установка Python
curl -L -o python.tar.gz https://github.com/indygreg/python-build-standalone/releases/download/20230826/cpython-3.10.13+20230826-x86_64-unknown-linux-gnu-install_only.tar.gz
tar -xzf python.tar.gz

# создание симлинков
ln -sf /tmp/python/bin/python3 /usr/local/bin/python3
ln -sf /tmp/python/bin/python3 /usr/local/bin/python

# установка pip
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3 get-pip.py

# симлинки для pip
ln -sf /tmp/python/bin/pip /usr/local/bin/pip
ln -sf /tmp/python/bin/pip3 /usr/local/bin/pip3

# установка MRJob
pip install mrjob

# проверка
python -c "import mrjob; print('MRJob установлен успешно!')"
```

#### Автоматическая установка на все nodemanager'ы
```bash
# выходим из контейнера
exit

# устанавливаем на все nodemanager'ы
for i in 1 2 3; do
  echo "Устанавливаем Python на nodemanager$i..."
  docker-compose exec nodemanager$i bash -c "
    cd /tmp && \
    curl -L -o python.tar.gz https://github.com/indygreg/python-build-standalone/releases/download/20230826/cpython-3.10.13+20230826-x86_64-unknown-linux-gnu-install_only.tar.gz && \
    tar -xzf python.tar.gz && \
    ln -sf /tmp/python/bin/python3 /usr/local/bin/python3 && \
    ln -sf /tmp/python/bin/python3 /usr/local/bin/python && \
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python3 get-pip.py && \
    ln -sf /tmp/python/bin/pip /usr/local/bin/pip && \
    ln -sf /tmp/python/bin/pip3 /usr/local/bin/pip3 && \
    pip install mrjob && \
    echo ' Python и MRJob установлены на nodemanager$i'
  "
done
```

### 6. Создание тестового скрипта MapReduce

```bash
# создаем скрипт в директории scripts
cat > scripts/simple_test.py << 'EOF'
#!/usr/bin/env python
from mrjob.job import MRJob

class SimpleTest(MRJob):

    def mapper(self, _, line):
        # Пропускаем заголовок
        if 'Transaction_ID' in line:
            return
            
        parts = line.split(',')
        if len(parts) >= 4:
            yield "total_lines", 1
            category = parts[3].strip()
            yield f"category_{category}", 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    SimpleTest.run()
EOF
```

### 7. Настройка конфигурации MRJob

```bash
# создаем конфигурационный файл для MRJob
cat > scripts/.mrjob.conf << 'EOF'
runners:
  hadoop:
    hadoop_bin: /opt/hadoop-3.2.1/bin/hadoop
    hadoop_streaming_jar: /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar
    hdfs_temp_dir: hdfs://namenode:9000/tmp/mrjob
    python_bin: /tmp/python/bin/python3
    hdfs_namenode: hdfs://namenode:9000

  local:
    python_bin: /tmp/python/bin/python3
EOF
```

### 8. Запуск первого MapReduce задания

```bash
# заходим в namenode
docker-compose exec namenode bash

# переходим в директорию со скриптами
cd /scripts

# запускаем локальный тест (без Hadoop)
python simple_test.py /data/input/retail_sales_dataset.csv

# запускаем на Hadoop кластере
python simple_test.py -r hadoop \
  hdfs://namenode:9000/user/root/input/retail_sales_dataset.csv \
  --output-dir hdfs://namenode:9000/user/root/output/first_test
```

### 9. Проверка результатов

```bash
# просмотр результатов
hdfs dfs -cat /user/root/output/first_test/part-*

# скачивание результатов
hdfs dfs -get /user/root/output/first_test/part-* /tmp/results/
cat /tmp/results/part-*

# проверка статуса YARN
yarn node -list
yarn application -list
```


### 10. Полезные команды для мониторинга

```bash
# статус HDFS
hdfs dfsadmin -report

# список приложений YARN
yarn application -list

# логи конкретного приложения
yarn logs -applicationId <application_id>

# дисковое пространство HDFS
hdfs dfs -df -h

# список файлов в HDFS
hdfs dfs -ls -R /user/root

# проверка здоровья кластера
docker-compose ps
docker-compose logs --tail=20 namenode
docker-compose logs --tail=20 resourcemanager
```

### 11. Остановка и перезапуск кластера

```bash
# остановка кластера
docker-compose down

# запуск кластера
docker-compose up -d

# полная перезагрузка (если есть проблемы)
docker-compose down -v
docker-compose up -d
```

**Следующие шаги:**
1. Проверьте что все контейнеры в статусе "healthy"
2. Запустите тестовый скрипт
3. Поэкспериментируйте с созданием собственных MapReduce задач
4. Изучите веб-интерфейсы: http://localhost:9871 (HDFS) и http://localhost:8088 (YARN)