# 使用官方MySQL 8.0.37的Debian镜像作为基础镜像
FROM mysql:8.0.37-debian

# 安装Zookeeper和必要的工具
RUN apt-get update && apt-get install -y \
    zookeeper \
    python3 \
    python3-pip \
    vim \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 创建Zookeeper的配置文件
RUN echo "tickTime=2000" > /etc/zookeeper/conf/zoo.cfg \
    && echo "dataDir=/var/lib/zookeeper" >> /etc/zookeeper/conf/zoo.cfg \
    && echo "clientPort=2181" >> /etc/zookeeper/conf/zoo.cfg \
    && echo "initLimit=5" >> /etc/zookeeper/conf/zoo.cfg \
    && echo "syncLimit=2" >> /etc/zookeeper/conf/zoo.cfg

# 环境变量配置，为MySQL服务设置
ENV MYSQL_ROOT_PASSWORD=123456
ENV MYSQL_DATABASE=database
ENV MYSQL_USER=user
ENV MYSQL_PASSWORD=123456

# 暴露MySQL和Zookeeper的端口
EXPOSE 3306 2181

# 设置默认启动命令为 Bash Shell
CMD ["bash"]
