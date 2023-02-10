package app.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PLMLABLE {
    private int id;
    private String project_num;
    private String workcode;
    private String position_code;
    private String workname;
    private String note;
    private long createtime;
    private long actual_start_time_3d;
    private long actual_finish_time_3d;
    private long actual_start_time_2d;
    private long actual_finish_time_2d;
    private String actual_drawing_num;
    private String multiplexing_drawing_num;
    private String type;
    private String part_out;
    private String draw_improve;
    private String ehnum;
    private String performer;
    private String call_draw_amount;
    private long actual_mc_receive_time;
    private long send_drawing_time;

}