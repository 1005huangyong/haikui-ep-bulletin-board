package app.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DWSDesignUndeterminedAbnormalBean {
    private int id;
    private String project_num;
    private String station_num;
    private String station_name;
    private int plan_drawings;
    private int actual_numbers;
    private String plan_draw_time;
    private String actual_draw_time;
}
