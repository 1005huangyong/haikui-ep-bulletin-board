package app.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DWSDrawingDetails {
    private int id;
    private String project_num;
    private String position_code;
    private String mission;
    private int plan_number;
    private int actual_number;
    private Date mc_plan_accept_time;
    private Date send_drawing_time;

}