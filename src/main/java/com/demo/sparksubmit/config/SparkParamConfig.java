package com.demo.sparksubmit.config;

import com.demo.sparksubmit.common.Separator;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @Created by IntelliJ IDEA.
 * @author: MikeLiu
 * @Date: 2020/7/20
 * @Time: 16:25
 * @des:
 */
@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "sparkparam")
public class SparkParamConfig {
    // owner
    private String owner;
    // job name
    private String jobname;
    // file path
    private Map<String, List<String>> filepath;

    public void print() {
        System.out.println(this.toString());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("owner:")
                .append(owner)
                .append(Separator.ENTER)
                .append("jobname:")
                .append(jobname)
                .append(Separator.ENTER);
        for (String key : filepath.keySet()) {
            List<String> files = filepath.get(key);
            for (int i = 0; i < files.size(); i ++) {
                sb.append(key + (i + 1) + ":")
                        .append(files.get(i))
                        .append(Separator.ENTER);
            }
        }
        return sb.toString();
    }

}
