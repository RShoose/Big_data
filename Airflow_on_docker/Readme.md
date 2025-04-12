# Инструкция по запуску Airflow на Docker в VsCode

1. Скачайте и установите Docker Desktop с официального 
сайта https://www.docker.com/products/docker-desktop/

2. Установите расширения VS Code:
    - Docker (Microsoft)
    - Dev  Containers
    - Python
    - YAML

3. Создайте рабочую директорию в VS Code:
    - File -> Open Folder
    - Создайте новую папку (например, airflow-project)
    - Выберите эту папку

3. Создайте структуру проекта:  
    airflow-project/  
    ├── dags/  
    ├── logs/  
    ├── plugins/  
    ├── docker-compose.yaml  
    └── .env  

4. Создайте docker-compose.yaml:
    - Скачайте официальный файл: https://airflow.apache.org/docs/apache-airflow/2.10.3/docker-compose.yaml
    - Или создайте новый файл и скопируйте содержимое

    На самом деле в данном файле нужно внести изменения для использования LocalExecutor и PostgreSQL, такие как:
    ```
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    ```
    - Также стоит проверить пути к нужным директориям:
    ```
    volumes: 
    - ./dags:/opt/airflow/dags 
    # DAG файлы  
    - ./logs:/opt/airflow/logs 
    # Логи 
    - ./plugins:/opt/airflow/plugins 
    # Плагины 
    - ./requirements.txt:/requirements.txt 
    # Зависимости Python
    ```

5. Создайте .env файл:  
    Пропишите в нем:  
    AIRFLOW_UID=50000  
    *устанавливается для решения проблем с правами доступа в контейнерах Docker.*

6. Создать dockerfile:

    ```
    FROM apache/airflow:2.10.3

    USER root

    # Установка системных зависимостей
    RUN apt-get update && apt-get install -y \
        python3-dev \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

    # Копирование файла requirements.txt
    COPY requirements.txt /requirements.txt

    # Установка Python пакетов
    USER airflow
    RUN pip install --no-cache-dir --only-binary :all: -r /requirements.txt```


7. Соберем образ:
    ```docker build -t cv-airflow .```

    **Измените AIRFLOW_IMAGE_NAME в docker-compose.yaml на cv-airflow**  
   После сборки выполните инициализацию базы данных и пользователя:  
    ```docker-compose up airflow-init```  
   После успешной инициализации, запустите webserver и scheduler:  
   ``` docker-compose up -d```  

9. Создайте тестовый DAG в директории dags/

10. Проверьте работу Airflow:
    - Откройте браузер и перейдите по адресу http://localhost:8080
    - Логин: airflow
    - Пароль: airflow

Структура проекта должна выглядеть так:  
 
airflow_project/        
├── dags/       
│   └── test_dag.py     
├── logs/       
├── plugins/        
├── docker-compose.yaml     
├── dockerfile      
├── requirements.txt        
└── .env        
