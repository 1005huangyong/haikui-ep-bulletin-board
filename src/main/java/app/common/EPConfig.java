package app.common;

public class EPConfig {
     //mysql
     public static final String mysql_host="10.22.1.70";
     public static final int mysql_port=3306;
     public static final String mysql_database="easy_project22";
     public static final String mysql_user="root";
     public static final String mysql_password="Lyric@123";
     public static final String mysql_timezone="Asia/Shanghai";
     public static final String tableNames =
     "easy_project22.project,easy_project22.project_plan,easy_project22.PLM_LABLE,easy_project22.military_draw," +
     "easy_project22.project_station";


/*     public static final String mysql_host="192.168.0.205";
     public static final int mysql_port=3306;
     public static final String mysql_database="BlueIot";
     public static final String mysql_user="cdc";
     public static final String mysql_password="CDC@123.com";
     public static final String mysql_timezone="Asia/Shanghai";*/


     //kafka
     public static final String BROKER="ecs-026:9092,ecs-027:9092,ecs-028:9092,ecs-029:9092";
     public static final String DEFAULT_TOPIC="default_topic";

     //clickhouse
     public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
     public static  final String CLICKHOUSE_URL="jdbc:clickhouse://192.168.0.209:8123/default";
     public static  final String URL = "192.168.0.209";
     public static  final int HORT = 8123;
     public static  final String DATABASE = "default";
     public static  final String USERNAME = "default";
     public static  final String PASSWORD = "haikui123";
}