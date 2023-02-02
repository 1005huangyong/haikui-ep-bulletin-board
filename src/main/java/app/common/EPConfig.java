package app.common;

public class EPConfig {
     //mysql
     public static final String mysql_host="10.22.1.70";
     public static final int mysql_port=3306;
     public static final String mysql_database="easy_project22";
     public static final String mysql_user="sunwei";
     public static final String mysql_password="Lyric@sunwei";
     public static final String mysql_timezone="Asia/Shanghai";

/*
     public static final String mysql_host="192.168.0.205";
     public static final int mysql_port=3306;
     public static final String mysql_database="BlueIot";
     public static final String mysql_user="root";
     public static final String mysql_password="Haikui_mysql_8";
     public static final String mysql_timezone="Asia/Shanghai";*/


     //kafka
     public static final String BROKER="ecs-026:9092,ecs-027:9092,ecs-028:9092,ecs-029:9092";
     public static final String DEFAULT_TOPIC="default_topic";

     //clickhouse
     public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
     public static  final String CLICKHOUSE_URL="jdbc:clickhouse://127.0.0.1:8123/test";
}