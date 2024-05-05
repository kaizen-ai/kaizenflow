**Technology Used**
The primary technology used in my project, aside from Python and Jupyter notebooks, was
PySpark, the Python API for accessing Apache Spark.  Apache Spark is a framework designed
for quickly processing large datasets, and can be used in conjunction with a wide array
of languages (such as Java, Python, and R) for a multitude of applications, such as SQL,
machine learning, and real-time data processing (RTDP).  PySpark allows the user to
access this powerful framework.  

Apache Spark is centered around Resilient Distributed Datasets (RDD's hereafter), which
allow users to split a large (and immutable) dataset between multiple machines, thereby
working around storage limitations (Pointer, 2024).  Among the many advantages of Apache
Spark are its capability to quickly perform advanced analytics, support for multiple 
programming languages (as stated previously), flexibility, and "detailed documentation...
[which] provides detailed tutorials and examples that explain complex concepts clearly
and concisely" ("The Good", 2023).  However, with these high performance specs comes 
higher computational costs.  Additionally, Apache Spark is known to have trouble with
larger collections of small files and relies on external storage ("The Good", 2023).

*Similar Technologies*
Other Apache technologies such as Hadoop and Flink offer similar data processing
capabilities.  Hadoop handles large datasets more effectively than Spark, but at the
cost of time ("The Good", 2023).  Flink, like Spark, supports multiple programming 
languages, but Spark is superior in terms of ease of use (Mohan & Thyagarajan, 2023).




Pointer, 2024: https://www.infoworld.com/article/3236869/what-is-apache-spark-the-big-data-platform-that-crushed-hadoop.html
"The Good", 2023: https://www.altexsoft.com/blog/apache-spark-pros-cons/
Mohan, Thyagarajan, 2023: https://aws.amazon.com/blogs/big-data/a-side-by-side-comparison-of-apache-spark-and-apache-flink-for-common-streaming-use-cases/#:~:text=Spark%20is%20known%20for%20its,and%20low%2Dlatency%20stateful%20computations.
