package app.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Project {
private String id;
private String project_num;
private String project_name;
private String status;
private String client_num;
private String client_name;
}