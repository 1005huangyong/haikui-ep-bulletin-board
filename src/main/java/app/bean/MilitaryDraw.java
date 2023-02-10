package app.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class MilitaryDraw {
    private int id;
    private String project_num;
    private String position_code;
    private String mission;
    private int plan_number;
    private int reuse_number;
    private String vip_position;
    private String start_time;
    private long mc_plan_accept_time;
    private long mc_actual_accept_time;
    private String designer;
    private String frameworker;
    private String manager;
    private String type;
    private int actual_number;
    private String remark;
    private int design_drawing_number;
    private String creator;
    private long create_time;
    private String modifier;
    private long modify_time;
    private int deleted;
    private String deleted_reason;
    private String deleter;
    private long deleted_time;
    private int military_id;
}