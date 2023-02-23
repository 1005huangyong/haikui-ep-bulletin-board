package app.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DWSProjectDataTest {
    private int id;
    private String project_num;
    private String station_num ;
    private String station_name ;
    private int drawings ;
    private int real_drawings;
}
