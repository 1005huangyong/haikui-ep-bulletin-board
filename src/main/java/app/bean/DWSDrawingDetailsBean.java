package app.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DWSDrawingDetailsBean {
    private int id;
    private String project_num;
    private String position_code;
    private String mission;
    private int plan_number;
    private int actual_number;
    private String mc_plan_accept_time;
    //private String send_drawing_time;

}