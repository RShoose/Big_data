#!/bin/bash
set -e

echo "=== Настройка Hive для лекции ==="

# Фиксим guava - только копируем с хоста
echo "Копирование guava..."
cp /host/guava.jar /opt/hive/lib/guava-27.0-jre.jar
cp /host/guava.jar /opt/hadoop-3.2.1/share/hadoop/common/lib/

# Удаляем конфликтующие версии
rm -f /opt/hadoop-3.2.1/share/hadoop/hdfs/lib/guava-*.jar 2>/dev/null || true

# Конфигурация Hive
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
    <value>hdfs://namenode-lecture:8020/user/hive/warehouse</value>  # Меняем здесь
  </property>
</configuration>
XML

# Конфигурация Hadoop для внешнего HDFS
mkdir -p /opt/hadoop/etc/hadoop
cat > /opt/hadoop/etc/hadoop/core-site.xml << 'XML'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode-lecture:8020</value>  # Меняем здесь
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
echo "  docker exec -it hive-lecture /opt/hive/bin/hive"  # Меняем здесь
echo ""
echo "Пример команд:"
echo "  SHOW DATABASES;"
echo "  CREATE DATABASE demo;"
echo "  USE demo;"
echo "  CREATE TABLE users (id INT, name STRING);"
echo ""

tail -f /dev/null
