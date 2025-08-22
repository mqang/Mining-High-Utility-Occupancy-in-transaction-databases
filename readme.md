- Đối với file code chạy thuật toán thuần Pandas thì có thể chạy bằng Colab của Google cho tiện

- Còn đối với phiên bản code Spark thì ta cần set up như sau:
### Bước 1: Cài đặt các phần mềm yêu cầu

Mở Terminal (Ctrl+Alt+T) và thực hiện các lệnh sau.

#### 1.1. Cài đặt Java

```bash
sudo apt update
sudo apt install -y openjdk-8-jdk
```

#### 1.2. Cài đặt và cấu hình SSH

```bash
sudo apt install -y ssh openssh-server
```

Tạo cặp khóa SSH để có thể đăng nhập không cần mật khẩu vào `localhost`:
```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
```

Kiểm tra bằng cách thử SSH vào localhost. Lần đầu tiên có thể hỏi xác nhận, gõ `yes`.
```bash
ssh localhost
# Gõ 'exit' để thoát khỏi phiên SSH
exit
```

#### 1.3. Tải và giải nén Hadoop & Spark

Tải và giải nén các phiên bản đã được kiểm chứng với dự án này vào thư mục `/opt`.

```bash
# Tải Hadoop 3.2.1
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz
# Tải Spark 3.1.1
wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz

# Giải nén
sudo tar -xzf hadoop-3.2.1.tar.gz -C /opt/
sudo tar -xzf spark-3.1.1-bin-hadoop3.2.tgz -C /opt/

# Đổi tên thư mục cho gọn
sudo mv /opt/hadoop-3.2.1 /opt/hadoop
sudo mv /opt/spark-3.1.1-bin-hadoop3.2 /opt/spark

# Cấp quyền sở hữu cho người dùng hiện tại
sudo chown -R $USER:$USER /opt/hadoop
sudo chown -R $USER:$USER /opt/spark
```

### Bước 2: Cấu hình môi trường

#### 2.1. Thiết lập biến môi trường

Mở file `.bashrc` để thêm các biến môi trường.

```bash
nano ~/.bashrc
```

Thêm các dòng sau vào cuối file:
```bash
# HADOOP & SPARK VARIABLES
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin
```

Áp dụng các thay đổi:
```bash
source ~/.bashrc
```

#### 2.2. Cấu hình Hadoop

Chỉnh sửa một số file XML trong `$HADOOP_HOME/etc/hadoop`.

1.  **`hadoop-env.sh`**:
    ```bash
    nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh
    ```
    Tìm và sửa dòng `export JAVA_HOME` thành: `export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64`

2.  **`core-site.xml`**:
    ```bash
    nano $HADOOP_HOME/etc/hadoop/core-site.xml
    ```
    Thêm vào giữa cặp thẻ `<configuration>`:
    ```xml
    <property>
      <name>fs.defaultFS</name>
      <value>hdfs://localhost:9000</value>
    </property>
    ```

3.  **`hdfs-site.xml`**:
    ```bash
    nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
    ```
    Thêm vào giữa cặp thẻ `<configuration>`:
    ```xml
    <property>
      <name>dfs.replication</name>
      <value>1</value>
    </property>
    <property>
      <name>dfs.namenode.name.dir</name>
      <value>file:///opt/hadoop/data/namenode</value>
    </property>
    <property>
      <name>dfs.datanode.data.dir</name>
      <value>file:///opt/hadoop/data/datanode</value>
    </property>
    ```

4.  **`mapred-site.xml`**:
    ```bash
    nano $HADOOP_HOME/etc/hadoop/mapred-site.xml
    ```
    Thêm vào giữa cặp thẻ `<configuration>`:
    ```xml
    <property>
      <name>mapreduce.framework.name</name>
      <value>yarn</value>
    </property>
    ```

5.  **`yarn-site.xml`**:
    ```bash
    nano $HADOOP_HOME/etc/hadoop/yarn-site.xml
    ```
    Thêm vào giữa cặp thẻ `<configuration>`:
    ```xml
    <property>
      <name>yarn.nodemanager.aux-services</name>
      <value>mapreduce_shuffle</value>
    </property>
    <property>
      <name>yarn.resourcemanager.hostname</name>
      <value>localhost</value>
    </property>
    ```

#### 2.3. Cấu hình Spark

Chỉnh sửa file log của Spark để output gọn gàng.

```bash
cp $SPARK_HOME/conf/log4j.properties.template $SPARK_HOME/conf/log4j.properties
nano $SPARK_HOME/conf/log4j.properties
```

Tìm dòng `log4j.rootCategory=INFO, console` và đổi thành `log4j.rootCategory=WARN, console`.

#### 2.4. Format NameNode

Lệnh này chỉ thực hiện **MỘT LẦN DUY NHẤT** khi cài đặt. Nó sẽ xóa mọi dữ liệu HDFS cũ.

```bash
hdfs namenode -format
```

### Bước 3: Chạy dự án

#### 3.1.Cài đặt thư viện Python

# Cài đặt các thư viện Python
```
sudo apt install -y python3-pip
pip3 install -r requirements.txt
```

#### 3.2. Khởi động các dịch vụ

```bash
# Khởi động HDFS
start-dfs.sh

# Khởi động YARN
start-yarn.sh

# Khởi động Spark History Server
start-history-server.sh
```

Kiểm tra các dịch vụ đang chạy bằng lệnh `jps`. Sẽ thấy các tiến trình như `NameNode`, `DataNode`, `ResourceManager`, `NodeManager`, `HistoryServer`.

#### 3.3. Chuẩn bị dữ liệu trên HDFS

```bash
# Tạo thư mục trên HDFS (nếu chưa có)
hdfs dfs -mkdir -p /user/$USER/input

# Đưa các file dữ liệu từ thư mục dự án lên HDFS
hdfs dfs -put data/*.csv /user/$USER/input/
hdfs dfs -put data/*.txt /user/$USER/input/
hdfs dfs -put data/*.dat /user/$USER/input/
```

#### 3.4. Chỉnh sửa và thực thi Script

Mở file `run_comparison.py` và chỉnh sửa các tham số trong hàm `main()` để chọn dataset muốn chạy.

**Ví dụ, để chạy với `OnlineRetail`:**

Thực thi script:
```bash
spark-submit --master yarn --deploy-mode client Spark_OnlineRetail.py
```

### Bước 4: Xem kết quả

1.  **Trên Terminal:** Theo dõi các bảng kết quả trung gian và bảng so sánh cuối cùng.
2.  **Xem biểu đồ:** Sau khi script chạy xong, một file ảnh (ví dụ: `onlineretail_performance_comparison.png`) sẽ được tạo ra trong thư mục dự án. Mở file này để xem biểu đồ.
3.  **Dừng các dịch vụ:** Khi đã hoàn thành, dừng các dịch vụ.
    ```bash
    stop-all.sh
    stop-history-server.sh
    ```