package app.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

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
    private LocalDateTime start_time;
    private LocalDateTime mc_plan_accept_time;
    private LocalDateTime mc_actual_accept_time;
    private String designer;
    private String frameworker;
    private String manager;
    private String type;
    private int actual_number;
    private String remark;
    private int design_drawing_number;
    private String creator;
    private LocalDateTime create_time;
    private String modifier;
    private LocalDateTime modify_time;
    private int deleted;
    private String deleted_reason;
    private String deleter;
    private LocalDateTime deleted_time;
    private int military_id;
}