package app.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProjectStation {
    private int id;
    private int project_id;
    private String project_num;
    private String station_num ;
    private String station_name ;
    private int drawings ;
    private int real_drawings;
}
